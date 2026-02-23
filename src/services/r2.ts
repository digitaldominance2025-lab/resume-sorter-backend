// src/services/r2.ts
import fs from "fs";
import path from "path";
import https from "https";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";

type R2Env = {
  accountId: string;
  accessKeyId: string;
  secretAccessKey: string;
  bucket: string;
};

let _client: S3Client | null = null;
let _bucket: string | null = null;

function readR2Env(): R2Env {
  const accountId = (process.env.CLOUDFLARE_R2_ACCOUNT_ID || "").trim();
  const accessKeyId = (process.env.CLOUDFLARE_R2_ACCESS_KEY_ID || "").trim();
  const secretAccessKey = (process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY || "").trim();
  const bucket = (process.env.CLOUDFLARE_R2_BUCKET || "").trim();

  if (!accountId) throw new Error("Missing CLOUDFLARE_R2_ACCOUNT_ID");
  if (!accessKeyId) throw new Error("Missing CLOUDFLARE_R2_ACCESS_KEY_ID");
  if (!secretAccessKey) throw new Error("Missing CLOUDFLARE_R2_SECRET_ACCESS_KEY");
  if (!bucket) throw new Error("Missing CLOUDFLARE_R2_BUCKET");

  return { accountId, accessKeyId, secretAccessKey, bucket };
}

function getR2Client(): { client: S3Client; bucket: string } {
  if (_client && _bucket) return { client: _client, bucket: _bucket };

  const env = readR2Env();

  _bucket = env.bucket;
  _client = new S3Client({
    region: "auto",
    endpoint: `https://${env.accountId}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: env.accessKeyId,
      secretAccessKey: env.secretAccessKey,
    },

    // R2 is most reliable with path-style
    forcePathStyle: true,

    // Force stable TLS handshake settings
    requestHandler: new NodeHttpHandler({
      httpsAgent: new https.Agent({
        keepAlive: true,
        minVersion: "TLSv1.2",
      }),
    }),
  });

  return { client: _client, bucket: _bucket };
}

function guessContentType(p: string): string {
  const ext = path.extname(p).toLowerCase();
  if (ext === ".pdf") return "application/pdf";
  if (ext === ".txt") return "text/plain";
  if (ext === ".json") return "application/json";
  if (ext === ".doc") return "application/msword";
  if (ext === ".docx")
    return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  return "application/octet-stream";
}

function streamToBuffer(stream: any): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on("data", (chunk: Buffer) => chunks.push(Buffer.from(chunk)));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks)));
  });
}

// ----------------------------
// Public API
// ----------------------------
export async function r2UploadFromLocalFile(args: {
  localPath: string;
  key: string;
  contentType?: string;
}): Promise<{ bucket: string; key: string }> {
  const { localPath, key, contentType } = args;

  if (!fs.existsSync(localPath)) {
    throw new Error(`Local file not found: ${localPath}`);
  }

  const { client, bucket } = getR2Client();
  const body = fs.createReadStream(localPath);

  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: contentType || guessContentType(localPath),
    })
  );

  return { bucket, key };
}

// ✅ NEW: Upload a Buffer directly to R2 (cloud-only, Vercel-friendly)
export async function r2UploadBuffer(args: {
  buffer: Buffer;
  key: string;
  contentType?: string;
}): Promise<{ bucket: string; key: string }> {
  const { buffer, key, contentType } = args;
  if (!buffer || !Buffer.isBuffer(buffer)) throw new Error("Missing buffer");
  if (!key) throw new Error("Missing key");

  const { client, bucket } = getR2Client();

  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: buffer,
      ContentType: contentType || "application/octet-stream",
    })
  );

  return { bucket, key };
}

export async function r2DownloadToBuffer(args: { key: string }): Promise<Buffer> {
  const { key } = args;
  if (!key) throw new Error("Missing key");

  const { client, bucket } = getR2Client();

  const resp = await client.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    })
  );

  // @ts-ignore
  if (!resp.Body) throw new Error("R2 object body missing");
  // @ts-ignore
  return streamToBuffer(resp.Body);
}

// convenience: old-style signature
export async function r2DownloadToBufferKey(key: string): Promise<Buffer> {
  return r2DownloadToBuffer({ key });
}
export function getPublicR2Url(key: string) {
  const accountId = process.env.R2_ACCOUNT_ID;
  const bucket = process.env.CLOUDFLARE_R2_BUCKET;
  return `https://${accountId}.r2.cloudflarestorage.com/${bucket}/${key}`;
}
