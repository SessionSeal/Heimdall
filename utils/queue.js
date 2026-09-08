/**
 * SQS nudge to wake Thor. The jobs table is the source of truth; this
 * message carries no state — if SQS is down, Thor's fallback poll still
 * picks the job up (just a little later).
 */
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const { SQS_URL, AWS_REGION } = require("../config");

const sqs = SQS_URL ? new SQSClient({ region: AWS_REGION }) : null;

async function nudgeThor(recordId) {
  if (!sqs) return;
  try {
    await sqs.send(new SendMessageCommand({
      QueueUrl: SQS_URL,
      MessageBody: JSON.stringify({ kind: "SEAL", record_id: recordId }),
    }));
  } catch (e) {
    console.log(`[heimdall] sqs nudge failed (thor will poll): ${e.message}`);
  }
}

module.exports = { nudgeThor };
