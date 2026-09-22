import { Worker } from "bullmq";
import { connection } from "./queue/connection.js";
import { processWebhookDeliveryJob } from "./processor.js";

export const worker = new Worker(
  "webhook-delivery",
  (job) => processWebhookDeliveryJob(job),
  {
    connection,
  },
);
