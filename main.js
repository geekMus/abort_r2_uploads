import fs from "fs";
import path from "path";
import readline from "readline";
import { S3Client, ListMultipartUploadsCommand, AbortMultipartUploadCommand } from "@aws-sdk/client-s3";

// 配置文件路径
const CONFIG_FILE = path.resolve("./config.json");

// 读取配置
let config = {};
if (fs.existsSync(CONFIG_FILE)) {
    config = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"));
}

// 如果配置缺失，通过命令行交互获取
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

function ask(question, defaultValue) {
    return new Promise(resolve => {
        const prompt = defaultValue ? `${question} (${defaultValue}): ` : `${question}: `;
        rl.question(prompt, answer => {
            resolve(answer || defaultValue);
        });
    });
}

async function main() {
    console.log("=====================================================");
    console.log("自动中止 R2 中未完成的 Multipart Uploads");
    console.log("=====================================================");

    const Bucket = await ask("请输入要操作的 Bucket 名称", config.Bucket);
    const Prefix = await ask("请输入要清理的前缀路径（可选）", config.Prefix);
    const Endpoint = await ask("请输入 Endpoint URL", config.Endpoint);
    const AccessKeyId = await ask("请输入 R2 Access Key", config.AccessKeyId);
    const SecretAccessKey = await ask("请输入 R2 Secret Key", config.SecretAccessKey);

    rl.close();

    console.log("\n=====================================================");
    console.log("当前配置：");
    console.log(`Bucket: ${Bucket}`);
    console.log(`Prefix: ${Prefix}`);
    console.log(`Endpoint: ${Endpoint}`);
    console.log("=====================================================\n");

    const s3Client = new S3Client({
        endpoint: Endpoint,
        region: "auto",
        credentials: { accessKeyId: AccessKeyId, secretAccessKey: SecretAccessKey },
    });

    let failedUploads = [];

    try {
        console.log("正在获取未完成上传列表...\n");

        const listCmd = new ListMultipartUploadsCommand({
            Bucket,
            Prefix
        });

        const listResult = await s3Client.send(listCmd);

        const uploads = listResult.Uploads || [];
        if (uploads.length === 0) {
            console.log("未找到任何未完成的 multipart 上传.");
            return;
        }

        for (const u of uploads) {
            console.log("-----------------------------------------------------");
            console.log(`正在处理中: ${u.Key}`);
            console.log(`UploadId: ${u.UploadId}`);

            try {
                const abortCmd = new AbortMultipartUploadCommand({
                    Bucket,
                    Key: u.Key,
                    UploadId: u.UploadId
                });
                await s3Client.send(abortCmd);
                console.log(`中止成功: ${u.Key}`);
            } catch (err) {
                console.log(`中止失败: ${u.Key}`);
                failedUploads.push(u.Key);
            }
        }

    } catch (err) {
        console.error("获取未完成上传列表失败:", err.message);
        return;
    }

    if (failedUploads.length > 0) {
        const failedFile = path.resolve("failed_uploads.txt");
        fs.writeFileSync(failedFile, failedUploads.join("\n"), "utf8");
        console.log("\n部分上传中止失败，请查看 failed_uploads.txt");
    }

    console.log("\n=====================================================");
    console.log("所有未完成上传处理完成");
    console.log("=====================================================");
}

main();
