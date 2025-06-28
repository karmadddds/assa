import ffmpeg
import tempfile
import os
import time
import asyncio
from telethon import TelegramClient
from telethon.tl.types import PeerChannel, DocumentAttributeVideo
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.errors import FloodWaitError

# ✅ API credentials
api_id = '29588884'
api_hash = '519a3c04634144c1c3720dc11d9d9d43'
session_name = 'session_name'

# ✅ Source dan target channel
source_channel_id = 2579755803
target_channel_link = "https://t.me/+6U_7qiWQpLkxY2Rh"
start_message = 1660
end_message = 2500

# ✅ Batas ukuran file
MAX_FILE_SIZE_MB = 1700
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

async def get_video_metadata(video_path):
    try:
        probe = ffmpeg.probe(video_path)
        video_stream = next((stream for stream in probe["streams"] if stream["codec_type"] == "video"), None)

        if video_stream:
            duration = int(float(video_stream["duration"]))
            width = int(video_stream["width"])
            height = int(video_stream["height"])

            thumb_path = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg").name
            (
                ffmpeg.input(video_path, ss=1)
                .output(thumb_path, vframes=1, format="image2", vcodec="mjpeg")
                .run(capture_stdout=True, capture_stderr=True, overwrite_output=True)
            )
            return duration, width, height, thumb_path
    except Exception as e:
        print(f"⚠️ Gagal mendapatkan metadata video: {e}")
    return 0, 1280, 720, None

async def download_and_send_video(message, target, client, semaphore):
    async with semaphore:
        try:
            if message.file.size > MAX_FILE_SIZE_BYTES:
                print(f"⏭️ Video {message.id} dilewati (ukuran terlalu besar).")
                return

            start_time = time.time()
            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as temp_file:
                file_path = temp_file.name
                await message.download_media(file=file_path)
            download_time = time.time() - start_time

            duration, width, height, thumb_path = await get_video_metadata(file_path)

            start_upload = time.time()
            for attempt in range(3):
                try:
                    await client.send_file(
                        target,
                        file_path,
                        caption=message.text or "Video no caption",
                        attributes=[DocumentAttributeVideo(
                            duration=duration,
                            w=width,
                            h=height,
                            supports_streaming=True
                        )],
                        thumb=thumb_path if thumb_path else None
                    )
                    break
                except Exception as e:
                    print(f"⚠️ Upload gagal ({message.id}) percobaan ke-{attempt+1}: {e}")
                    await asyncio.sleep(10)

            upload_time = time.time() - start_upload

            os.remove(file_path)
            if thumb_path:
                os.remove(thumb_path)

            with open("last_id.txt", "w") as f:
                f.write(str(message.id))

            print(f"✅ Video {message.id} selesai ({download_time:.2f}s download, {upload_time:.2f}s upload)")

        except Exception as e:
            print(f"❌ Error video {message.id}: {e}")

async def main():
    semaphore = asyncio.Semaphore(5)  # Maksimal 5 video diproses paralel

    async with TelegramClient(session_name, api_id, api_hash) as client:
        try:
            source_channel = await client.get_entity(PeerChannel(source_channel_id))
            print(f"📌 Terhubung ke source channel ID: {source_channel_id}")

            try:
                await client(JoinChannelRequest(target_channel_link))
            except:
                pass  # Sudah join atau link invalid

            target_channel = await client.get_entity(target_channel_link)
            print(f"📌 Terhubung ke target channel: {target_channel.title}")

            # Resume dari file jika tersedia
            resume_id = start_message
            if os.path.exists("last_id.txt"):
                with open("last_id.txt", "r") as f:
                    resume_id = int(f.read()) + 1
                    print(f"⏩ Melanjutkan dari ID {resume_id}")

            tasks = []
            async for message in client.iter_messages(
                source_channel,
                reverse=True,
                offset_id=resume_id - 1,
                limit=end_message - resume_id + 1
            ):
                if message.video:
                    print(f"🎥 Queue video {message.id}...")
                    task = download_and_send_video(message, target_channel, client, semaphore)
                    tasks.append(task)

            await asyncio.gather(*tasks)

        except FloodWaitError as e:
            print(f"⚠️ Rate limit! Menunggu {e.seconds} detik...")
            await asyncio.sleep(e.seconds)
            await main()
        except Exception as e:
            print(f"❌ Error utama: {e}")

if __name__ == "__main__":
    asyncio.run(main())
