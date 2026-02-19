import 'dotenv/config';

function requireEnv(varName: string): string {
  const value = process.env[varName];
  if (!value) {
    throw new Error(`Missing required environment variable: ${varName}`);
  }
  return value;
}

export const config = {
  aws: {
    accessKeyId: requireEnv('AWS_ACCESS_KEY_ID'),
    secretAccessKey: requireEnv('AWS_SECRET_ACCESS_KEY'),
    region: requireEnv('AWS_REGION'),
    endpoint: process.env.AWS_ENDPOINT_URL || undefined,
  },
};
