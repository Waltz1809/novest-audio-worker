# Novest Audio Worker

GPU Worker tự động lấy text từ Novest API, gen audio qua VieNeu-TTS, convert sang HLS và upload lên Cloudflare R2.

## Cài đặt

1. Đảm bảo đã cài Docker và Docker Compose.
2. Clone repo này.
3. Copy .env.example thành .env và điền thông tin (đặc biệt là WORKER_TOTP_SECRET).
4. Chạy lệnh:
   `ash
   docker-compose up -d --build
   `

## Hoạt động

Worker sẽ tự động:
1. Gọi GET /api/worker/tasks để lấy chapter mới.
2. Gen audio.
3. Upload segments lên R2 thông qua presigned URLs cấp bởi Novest API.
4. Báo cáo hoàn thành qua POST /api/worker/complete.

