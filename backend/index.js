const express = require("express");
const { Pool } = require("pg");
const { slugify } = require("./utils");
const { connectRabbit, getChannel } = require("./rabbit");

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

// #region DB Connect
// #endregion

const pool = new Pool({
  host: "localhost",
  port: 5432,
  user: "postgres",
  password: "postgres",
  database: "assesment_rudex",
});

pool
  .query("SELECT 1")
  .then(() => {
    console.log("DB Connected");
  })
  .catch((err) => {
    console.error(err);
  });

// #region Migration
// #endregion

async function migrate() {
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
        await pool.query("BEGIN");

        const { rows } = await pool.query(
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

        await pool.query("COMMIT");

        return res.status(201).json({
          status: "ok",
          message: "News stored and queued",
          id: news.id,
        });
      } catch (error) {
        await pool.query("ROLLBACK");
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

    const count = await pool.query(
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
    const { rows } = await pool.query(dataQ, dataV);

    return res.json({
      page,
      limit,
      total,
      data: rows,
    });
  });

console.log("running at port 3000");

app.listen(3000);
