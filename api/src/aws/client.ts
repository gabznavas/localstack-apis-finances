import {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  ListBucketsCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueUrlCommand,
  ListQueuesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import type { Readable } from 'stream';
import { config } from '../config';

export type SqsMessageHandler = (
  body: string,
  context: { messageId: string; receiptHandle: string }
) => void | Promise<void>;

export interface ConsumeQueueOptions {
  /** Long polling: segundos que o ReceiveMessage espera por novas mensagens (máx 20). Default 20. */
  waitTimeSeconds?: number;
  /** Máximo de mensagens por chamada (1–10). Default 10. */
  maxMessagesPerPoll?: number;
  /** Se true (default), remove da fila só após o handler concluir sem erro. */
  deleteAfterProcess?: boolean;
  /** Para encerrar o loop: ex. AbortController.signal ou AbortSignal.timeout(60000). */
  signal?: AbortSignal;
}

const clientConfig = {
  region: config.aws.region,
  credentials: {
    accessKeyId: config.aws.accessKeyId,
    secretAccessKey: config.aws.secretAccessKey,
  },
  ...(config.aws.endpoint && { endpoint: config.aws.endpoint }),
  forcePathStyle: true,
};

export class AwsClient {
  private readonly s3: S3Client;
  private readonly sqs: SQSClient;

  constructor(overrides?: Partial<typeof clientConfig>) {
    const cfg = { ...clientConfig, ...overrides };
    this.s3 = new S3Client(cfg);
    this.sqs = new SQSClient(cfg);
  }

  // --- S3 ---

  /**
   * Cria o bucket somente se ainda não existir. Se já existir, não faz nada e não lança erro.
   */
  async createBucketIfNotExists(bucket: string): Promise<void> {
    const exists = await this.listBuckets().then((names) => names.includes(bucket));
    if (!exists) {
      await this.s3.send(new CreateBucketCommand({ Bucket: bucket }));
    }
  }

  async listBuckets(): Promise<string[]> {
    const { Buckets } = await this.s3.send(new ListBucketsCommand({}));
    return (Buckets ?? []).map((b) => b.Name!).filter(Boolean);
  }

  async putObject(
    bucket: string,
    key: string,
    body: string | Buffer | Uint8Array,
    contentType?: string
  ): Promise<void> {
    await this.s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentType: contentType,
      })
    );
  }

  async getObject(bucket: string, key: string): Promise<string> {
    const response = await this.s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: key })
    );
    const stream = response.Body as Readable;
    const chunks: Buffer[] = [];
    for await (const chunk of stream) chunks.push(Buffer.from(chunk));
    return Buffer.concat(chunks).toString('utf-8');
  }

  async getObjectBuffer(bucket: string, key: string): Promise<Buffer> {
    const response = await this.s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: key })
    );
    const stream = response.Body as Readable;
    const chunks: Buffer[] = [];
    for await (const chunk of stream) chunks.push(Buffer.from(chunk));
    return Buffer.concat(chunks);
  }

  async listObjects(bucket: string, prefix?: string): Promise<string[]> {
    const { Contents } = await this.s3.send(
      new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix })
    );
    return (Contents ?? []).map((o) => o.Key!).filter(Boolean);
  }

  async deleteObject(bucket: string, key: string): Promise<void> {
    await this.s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
  }

  async deleteBucket(bucket: string): Promise<void> {
    await this.s3.send(new DeleteBucketCommand({ Bucket: bucket }));
  }

  // --- SQS ---

  async createQueue(queueName: string, attributes?: Record<string, string>): Promise<string> {
    const { QueueUrl } = await this.sqs.send(
      new CreateQueueCommand({ QueueName: queueName, Attributes: attributes })
    );
    return QueueUrl!;
  }

  async getQueueUrl(queueName: string): Promise<string> {
    const { QueueUrl } = await this.sqs.send(
      new GetQueueUrlCommand({ QueueName: queueName })
    );
    if (!QueueUrl) throw new Error(`Fila não encontrada: ${queueName}`);
    return QueueUrl;
  }

  async listQueues(prefix?: string): Promise<string[]> {
    const { QueueUrls } = await this.sqs.send(
      new ListQueuesCommand({ QueueNamePrefix: prefix })
    );
    return QueueUrls ?? [];
  }

  async sendMessage(queueNameOrUrl: string, body: string, delaySeconds?: number): Promise<string> {
    const queueUrl = queueNameOrUrl.startsWith('http')
      ? queueNameOrUrl
      : await this.getQueueUrl(queueNameOrUrl);
    const result = await this.sqs.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: body,
        DelaySeconds: delaySeconds,
      })
    );
    return result.MessageId!;
  }

  async receiveMessages(
    queueNameOrUrl: string,
    maxMessages = 10,
    waitTimeSeconds = 5
  ): Promise<Array<{ messageId: string; body: string; receiptHandle: string }>> {
    const queueUrl = queueNameOrUrl.startsWith('http')
      ? queueNameOrUrl
      : await this.getQueueUrl(queueNameOrUrl);
    const { Messages } = await this.sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: waitTimeSeconds,
      })
    );
    return (Messages ?? []).map((m) => ({
      messageId: m.MessageId!,
      body: m.Body!,
      receiptHandle: m.ReceiptHandle!,
    }));
  }

  async deleteMessage(queueNameOrUrl: string, receiptHandle: string): Promise<void> {
    const queueUrl = queueNameOrUrl.startsWith('http')
      ? queueNameOrUrl
      : await this.getQueueUrl(queueNameOrUrl);
    await this.sqs.send(
      new DeleteMessageCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
      })
    );
  }

  /**
   * Loop de consumo: fica chamando receiveMessages (long polling), processa cada mensagem
   * com o handler e, se concluir sem erro, remove da fila. Use signal para parar o loop.
   */
  async consumeQueue(
    queueNameOrUrl: string,
    onMessage: SqsMessageHandler,
    options: ConsumeQueueOptions = {}
  ): Promise<void> {
    const {
      waitTimeSeconds = 20,
      maxMessagesPerPoll = 10,
      deleteAfterProcess = true,
      signal,
    } = options;

    const queueUrl = queueNameOrUrl.startsWith('http')
      ? queueNameOrUrl
      : await this.getQueueUrl(queueNameOrUrl);

    while (!signal?.aborted) {
      const messages = await this.receiveMessages(queueUrl, maxMessagesPerPoll, waitTimeSeconds);

      for (const msg of messages) {
        if (signal?.aborted) return;
        try {
          await onMessage(msg.body, { messageId: msg.messageId, receiptHandle: msg.receiptHandle });
          if (deleteAfterProcess) {
            await this.deleteMessage(queueUrl, msg.receiptHandle);
          }
        } catch (err) {
          // Não deleta: a mensagem volta à fila após o visibility timeout; segue para a próxima
          console.error('[consumeQueue] Erro ao processar mensagem', msg.messageId, err);
        }
      }
    }
  }

  async deleteQueue(queueNameOrUrl: string): Promise<void> {
    const queueUrl = queueNameOrUrl.startsWith('http')
      ? queueNameOrUrl
      : await this.getQueueUrl(queueNameOrUrl);
    await this.sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
  }

  get s3Client(): S3Client {
    return this.s3;
  }

  get sqsClient(): SQSClient {
    return this.sqs;
  }
}
