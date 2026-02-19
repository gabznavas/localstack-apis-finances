import { AwsClient } from './aws';

const aws = new AwsClient();

const controller = new AbortController();

async function main() {
  console.log('API rodando com TypeScript + Nodemon');

  // Exemplo S3
  // await aws.createBucketIfNotExists('meu-bucket');
  // await aws.putObject('meu-bucket', 'ola.txt', 'Olá, S3!');
  // const texto = await aws.getObject('meu-bucket', 'ola.txt');
  // console.log(texto)

  // Exemplo SQS
  const queueUrl = await aws.createQueue('minha-fila');
  await aws.sendMessage(queueUrl, JSON.stringify({ evento: 'teste' }));
  // const msgs = await aws.receiveMessages(queueUrl);

  process.on('SIGINT', () => controller.abort());

  await aws.consumeQueue(
    'minha-fila',
    async (body, { messageId }) => {
      const data = JSON.parse(body);
      console.log('Processando', messageId, data);
      // sua lógica aqui
    },
    {
      waitTimeSeconds: 20,
      signal: controller.signal,
    }
  );

}

main().catch(console.error);



