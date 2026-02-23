"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.r2UploadFromLocalFile = r2UploadFromLocalFile;
exports.r2UploadBuffer = r2UploadBuffer;
exports.r2DownloadToBuffer = r2DownloadToBuffer;
exports.r2DownloadToBufferKey = r2DownloadToBufferKey;
// src/services/r2.ts
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const https_1 = __importDefault(require("https"));
const client_s3_1 = require("@aws-sdk/client-s3");
const node_http_handler_1 = require("@aws-sdk/node-http-handler");
let _client = null;
let _bucket = null;
function readR2Env() {
    const accountId = (process.env.CLOUDFLARE_R2_ACCOUNT_ID || "").trim();
    const accessKeyId = (process.env.CLOUDFLARE_R2_ACCESS_KEY_ID || "").trim();
    const secretAccessKey = (process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY || "").trim();
    const bucket = (process.env.CLOUDFLARE_R2_BUCKET || "").trim();
    if (!accountId)
        throw new Error("Missing CLOUDFLARE_R2_ACCOUNT_ID");
    if (!accessKeyId)
        throw new Error("Missing CLOUDFLARE_R2_ACCESS_KEY_ID");
    if (!secretAccessKey)
        throw new Error("Missing CLOUDFLARE_R2_SECRET_ACCESS_KEY");
    if (!bucket)
        throw new Error("Missing CLOUDFLARE_R2_BUCKET");
    return { accountId, accessKeyId, secretAccessKey, bucket };
}
function getR2Client() {
    if (_client && _bucket)
        return { client: _client, bucket: _bucket };
    const env = readR2Env();
    _bucket = env.bucket;
    _client = new client_s3_1.S3Client({
        region: "auto",
        endpoint: `https://${env.accountId}.r2.cloudflarestorage.com`,
        credentials: {
            accessKeyId: env.accessKeyId,
            secretAccessKey: env.secretAccessKey,
        },
        // R2 is most reliable with path-style
        forcePathStyle: true,
        // Force stable TLS handshake settings
        requestHandler: new node_http_handler_1.NodeHttpHandler({
            httpsAgent: new https_1.default.Agent({
                keepAlive: true,
                minVersion: "TLSv1.2",
            }),
        }),
    });
    return { client: _client, bucket: _bucket };
}
function guessContentType(p) {
    const ext = path_1.default.extname(p).toLowerCase();
    if (ext === ".pdf")
        return "application/pdf";
    if (ext === ".txt")
        return "text/plain";
    if (ext === ".json")
        return "application/json";
    if (ext === ".doc")
        return "application/msword";
    if (ext === ".docx")
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    return "application/octet-stream";
}
function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks)));
    });
}
// ----------------------------
// Public API
// ----------------------------
async function r2UploadFromLocalFile(args) {
    const { localPath, key, contentType } = args;
    if (!fs_1.default.existsSync(localPath)) {
        throw new Error(`Local file not found: ${localPath}`);
    }
    const { client, bucket } = getR2Client();
    const body = fs_1.default.createReadStream(localPath);
    await client.send(new client_s3_1.PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentType: contentType || guessContentType(localPath),
    }));
    return { bucket, key };
}
// ✅ NEW: Upload a Buffer directly to R2 (cloud-only, Vercel-friendly)
async function r2UploadBuffer(args) {
    const { buffer, key, contentType } = args;
    if (!buffer || !Buffer.isBuffer(buffer))
        throw new Error("Missing buffer");
    if (!key)
        throw new Error("Missing key");
    const { client, bucket } = getR2Client();
    await client.send(new client_s3_1.PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: buffer,
        ContentType: contentType || "application/octet-stream",
    }));
    return { bucket, key };
}
async function r2DownloadToBuffer(args) {
    const { key } = args;
    if (!key)
        throw new Error("Missing key");
    const { client, bucket } = getR2Client();
    const resp = await client.send(new client_s3_1.GetObjectCommand({
        Bucket: bucket,
        Key: key,
    }));
    // @ts-ignore
    if (!resp.Body)
        throw new Error("R2 object body missing");
    // @ts-ignore
    return streamToBuffer(resp.Body);
}
// convenience: old-style signature
async function r2DownloadToBufferKey(key) {
    return r2DownloadToBuffer({ key });
}
