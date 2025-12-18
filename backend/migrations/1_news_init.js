module.exports = {
  name: "1_news_init",

  async up(client) {
    await client.query(`
            CREATE TABLE IF NOT EXISTS news (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                author TEXT NOT NULL,
                source TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT now()
            );
            `);
  },
};
