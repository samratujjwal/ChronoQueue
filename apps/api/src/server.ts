import { buildApp } from "./app.js";
import { config } from "./config/env.js";

const app = buildApp();

const start = async () => {
  try {
    await app.listen({
      port: config.PORT,
      host: config.HOST,
    });
  } catch (error) {
    app.log.error(error);
    process.exit(1);
  }
};

start();
