import { Queue } from "bullmq";
import { connection } from "./connection.js";

export const webhookQueue = new Queue("webhook-delivery", { connection });
