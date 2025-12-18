const express = require("express");
const { Pool } = require("pg");
const { slugify } = require("./utils");
const { connectRabbit, getChannel } = require("./rabbit");
const { Client } = require("@elastic/elasticsearch");

const app = express();

app.use(express.json());

// #region RMQ
// #endregion

(async () => {
  try {
    await connectRabbit();
  } catch (error) {
    console.error("Rabbit MQ connection error:", error);
    process.exit(1);
  }
})();

// #region ES
// #endregion

const es = new Client({
  node: process.env.ELASTICSEARCH_URL || "http://elasticsearch:9200",
});

async function waitForElasticsearch(retries = 10, delay = 3000) {
  while (retries > 0) {
    try {
      await es.ping();
      console.log("✅ Elasticsearch connected");
      return;
    } catch (err) {
      retries--;
      console.log(`⏳ Waiting for Elasticsearch... ${retries} attempts left`);
      await new Promise((res) => setTimeout(res, delay));
    }
  }
  throw new Error("❌ Could not connect to Elasticsearch after retries");
}

(async () => {
  await waitForElasticsearch();
  console.log("🚀 All services ready, starting API...");
})();

async function ensureIndex() {
  const exists = await es.indices.exists({ index: "news" });

  if (!exists) {
    await es.indices.create({
      index: "news",
      mappings: {
        properties: {
          title: { type: "text" },
          content: { type: "text" },
          author: { type: "text" },
          source: { type: "text" },
          created_at: { type: "date" },
        },
      },
    });

    console.log(">>> Created ES Index 'news'");
  }
}

(async () => {
  await ensureIndex();
})();

// (async () => {
//   await es.ping();
//   console.log(">>> Elasticsearch connected");
// })();

// #region DB Connect
// #endregion

let globalPool;

async function createPool(retries = 10, delay = 5000) {
  const newPool = new Pool({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT),
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
  });

  while (retries > 0) {
    try {
      await newPool.query("SELECT 1");
      console.log(">>> Postgres connected");
      globalPool = newPool;
      return newPool;
    } catch (error) {
      retries--;
      console.log(`>>> Waiting for PGSQL, ${retries} attempts left`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }

  throw new Error("<<< Postgres not available");
}

// async function waitForDb(retries = 10) {
//   while (retries) {
//     try {
//       await pool.query("SELECT 1");
//       console.log("✅ Postgres connected");
//       return;
//     } catch (err) {
//       retries--;
//       console.log("⏳ Waiting for Postgres...");
//       await new Promise((res) => setTimeout(res, 5000));
//     }
//   }
//   throw new Error("❌ Postgres not available");
// }

// waitForDb();

// #region Migration
// #endregion

async function migrate() {
  const pool = await createPool();
  const client = await pool.connect();

  try {
    console.log(">>> Running Migrations");

    await client.query(`
            CREATE TABLE IF NOT EXISTS migrations (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            executed_at TIMESTAMP NOT NULL DEFAULT now()
            );
            `);

    const { rows } = await client.query(`
        SELECT name FROM migrations
        `);
    const migrated = rows.map((r) => r.name);

    const migrations = [require("./migrations/1_news_init")];
    for (const migration of migrations) {
      if (!migrated.includes(migration.name)) {
        await migration.up(client);
        await client.query(`INSERT INTO migrations (name) VALUES ($1)`, [
          migration.name,
        ]);
      }
    }
    console.log(">>> Migrations Completed");
  } catch (error) {
    await client.query("ROLLBACK");
  } finally {
    client.release();
  }
}

// #region Health
// #endregion

migrate();

app.get("/health", (req, res) => {
  res.send("OK");
});

// #region News
// #endregion

app
  .route("/api/news")
  .post(async (req, res) => {
    const { title, content, author, source } = req.body;

    if (!title || !content || !author || !source) {
      return res.status(400).json({
        status: "error",
        message: "Missing required fields",
      });
    }

    const id = slugify(title);

    try {
      try {
        await globalPool.query("BEGIN");

        const { rows } = await globalPool.query(
          `
        INSERT INTO news (id, title, content, author, source)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
        `,
          [id, title, content, author, source]
        );

        const news = rows[0];

        // rmq q
        const rmcChannel = getChannel();
        rmcChannel.sendToQueue(
          "news_queue",
          Buffer.from(JSON.stringify(news)),
          { persistent: true }
        );

        await globalPool.query("COMMIT");

        return res.status(201).json({
          status: "ok",
          message: "News stored and queued",
          id: news.id,
        });
      } catch (error) {
        await globalPool.query("ROLLBACK");
        throw error;
      }
    } catch (error) {
      if (error.code === "23505") {
        return res.status(400).json({
          status: "error",
          message: "News already exists",
        });
      }

      console.error(error);
      return res.status(500).json({
        status: "error",
        message: "Something went wrong",
      });
    }
  })
  .get(async (req, res) => {
    const page = parseInt(req.query.page || 1, 10);
    const limit = parseInt(req.query.limit || 10, 10);
    const { source, author, search } = req.query;

    const offset = (page - 1) * limit;

    const values = [];
    let where = [];

    if (source) {
      values.push(source);
      where.push(`source = $${values.length}`);
    }

    if (author) {
      values.push(author);
      where.push(`author = $${values.length}`);
    }

    if (search) {
      values.push(`%${search}%`);
      where.push(`title ILIKE $${values.length}`);
    }

    if (where.length > 0) {
      where = `WHERE ${where.join(" AND ")}`;
    }

    const count = await globalPool.query(
      `SELECT COUNT(*)::int AS total FROM news ${where}`,
      values
    );

    const total = count.rows[0].total;

    const dataQ = `
    SELECT * FROM news
    ${where}
    ORDER BY created_at DESC
    LIMIT $${values.length + 1}
    OFFSET $${values.length + 2}
    `;

    const dataV = [...values, limit, offset];
    const { rows } = await globalPool.query(dataQ, dataV);

    return res.json({
      page,
      limit,
      total,
      data: rows,
    });
  });

app.get("/api/search", async (req, res) => {
  const { search } = req.query;

  if (!search) {
    res.status(400).json({
      status: "error",
      message: "Missing required query parameter: search",
    });
  }

  // search from elasticsearch
  const result = await es.search({
    index: "news",
    body: {
      query: {
        match: {
          title: search,
        },
      },
    },
  });

  const hits = result.hits.hits;

  let response = [];

  if (Array.isArray(hits)) {
    hits.forEach((hit) => {
      response.push({
        id: hit._id,
        title: hit._source.title,
        content: hit._source.content,
        author: hit._source.author,
        source: hit._source.source,
        created_at: hit._source.created_at,
      });
    });
  }

  return res.json(response);
});

console.log("running at port 3000");

app.listen(3000);
