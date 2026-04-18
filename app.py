"""
Novest Audio Tool — Giao diện web
===================================
Chạy: python app.py
Mở:  http://127.0.0.1:7861
"""

import json
from pathlib import Path

import gradio as gr

import tool

# ─── Helpers UI ────────────────────────────────────────────────────────────────

def _chapter_table(chapters: list[dict]) -> list[list]:
    return [
        [c["chapterId"], c["chapterTitle"], c["status"], c["wavFile"] if c["hasWav"] else "—", c["folder"]]
        for c in chapters
    ]


def _ready_folders() -> list[str]:
    """Danh sách folder có WAV nhưng chưa upload."""
    return [
        str(tool.CHAPTERS_DIR / c["folder"])
        for c in tool.list_chapters()
        if c["hasWav"] and not c["hasHls"]
    ]


def _all_folders() -> list[str]:
    return [
        str(tool.CHAPTERS_DIR / c["folder"])
        for c in tool.list_chapters()
    ]


# ─── Tab: Tải chương ───────────────────────────────────────────────────────────

def do_fetch(novel_id, limit, chapters_range):
    try:
        nid = int(novel_id) if str(novel_id).strip() else None
        cr  = chapters_range.strip() or None
        results = tool.fetch(novel_id=nid, chapters_range=cr, limit=int(limit))
    except Exception as e:
        return f"❌ Lỗi: {e}", gr.update()

    if not results:
        return "⚠️ Không có chương nào. Kiểm tra novel có bật membershipEnabled chưa?", gr.update()

    lines = [f"✓ Đã tải **{len(results)}** chương:\n"]
    for r in results:
        lines.append(
            f"- **[{r['chapterId']}]** {r['chapterTitle']}  \n"
            f"  `{r['wordCount']}` từ · ~{r['estMinutes']} phút audio  \n"
            f"  📁 `{r['folder']}`"
        )
    lines.append("\n---")
    lines.append("**Bước tiếp theo:**")
    lines.append("1. Mở VieNeu web UI: `uv run vieneu-web` (port 7860)")
    lines.append("2. Paste nội dung từ `content.txt` của folder tương ứng")
    lines.append("3. Gen audio → **Save WAV vào đúng folder đó**")
    lines.append("4. Chuyển sang tab **Upload** để upload lên R2")

    return "\n".join(lines), gr.update()


# ─── Tab: Upload ───────────────────────────────────────────────────────────────

def refresh_upload_list():
    folders = _ready_folders()
    if not folders:
        return gr.update(choices=[], value=None, label="Không có folder nào có WAV chờ upload")
    return gr.update(choices=folders, value=folders[0], label=f"Chọn folder ({len(folders)} chờ upload)")


def do_upload_one(folder_path):
    if not folder_path:
        return "⚠️ Chọn folder trước."
    log_lines = []
    try:
        result = tool.upload(folder_path, on_progress=lambda m: log_lines.append(m))
        log_lines.append(f"\n✅ XONG! [{result['chapterId']}] {result['chapterTitle']}")
        log_lines.append(f"   {result['duration']:.0f}s · {result['segments']} segments")
        log_lines.append(f"   R2 key: `{result['playlistKey']}`")
    except Exception as e:
        log_lines.append(f"\n❌ Lỗi: {e}")
    return "\n".join(log_lines)


def do_upload_all():
    chapters = [c for c in tool.list_chapters() if c["hasWav"] and not c["hasHls"]]
    if not chapters:
        return "⚠️ Không có folder nào có WAV chờ upload."

    log_lines = [f"Upload {len(chapters)} chương...\n"]
    ok = 0
    for ch in chapters:
        folder_path = str(tool.CHAPTERS_DIR / ch["folder"])
        log_lines.append(f"── [{ch['chapterId']}] {ch['chapterTitle']}")
        try:
            result = tool.upload(folder_path, on_progress=lambda m: log_lines.append(f"   {m}"))
            log_lines.append(f"   ✅ Xong\n")
            ok += 1
        except Exception as e:
            log_lines.append(f"   ❌ Lỗi: {e}\n")

    log_lines.append(f"─────\n✅ {ok}/{len(chapters)} chương hoàn thành.")
    return "\n".join(log_lines)


# ─── Tab: Trạng thái ───────────────────────────────────────────────────────────

def do_refresh_status():
    chapters = tool.list_chapters()
    if not chapters:
        return [], "chapters/ trống. Hãy Fetch trước."
    data = _chapter_table(chapters)
    done  = sum(1 for c in chapters if c["hasHls"])
    ready = sum(1 for c in chapters if c["hasWav"] and not c["hasHls"])
    wait  = sum(1 for c in chapters if not c["hasWav"])
    summary = f"**Tổng:** {len(chapters)}  |  ✅ Đã upload: {done}  |  🎵 Có WAV: {ready}  |  ⏳ Chờ WAV: {wait}"
    return data, summary


def do_reset(folder_path):
    if not folder_path:
        return "⚠️ Chọn folder trước."
    try:
        tool.reset(folder_path)
        meta = json.loads((Path(folder_path) / "meta.json").read_text(encoding="utf-8"))
        return f"✅ [{meta['chapterId']}] Reset về FAILED — sẽ xuất hiện lại ở lần Fetch tiếp theo."
    except Exception as e:
        return f"❌ Lỗi: {e}"


# ─── Build UI ──────────────────────────────────────────────────────────────────

with gr.Blocks(title="Novest Audio Tool", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 🎙️ Novest Audio Tool")
    gr.Markdown("Workflow: **Fetch** chương → Gen audio bằng VieNeu → **Upload** lên R2")

    with gr.Tabs():

        # ── Tab 1: Fetch ─────────────────────────────────────────────────────
        with gr.Tab("📥 Tải chương"):
            gr.Markdown("""
Lấy nội dung chương từ server về máy. Sau khi fetch, mỗi chương sẽ có một folder
riêng trong `chapters/` chứa `content.txt` để paste vào VieNeu và `meta.json` để tool
biết chương đó thuộc về đâu khi upload.
""")
            with gr.Row():
                inp_novel_id = gr.Number(label="Novel ID", precision=0, minimum=1)
                inp_limit    = gr.Slider(label="Số chương tối đa", minimum=1, maximum=50, value=10, step=1)
            inp_range  = gr.Textbox(label="Chapter ID range (tuỳ chọn)", placeholder="vd: 100-200")
            btn_fetch  = gr.Button("📥 Fetch", variant="primary")
            out_fetch  = gr.Markdown()

            btn_fetch.click(do_fetch, inputs=[inp_novel_id, inp_limit, inp_range], outputs=[out_fetch, out_fetch])

        # ── Tab 2: Upload ────────────────────────────────────────────────────
        with gr.Tab("🚀 Upload"):
            gr.Markdown("""
Chọn folder chương đã có file WAV rồi nhấn Upload.
Folder nào chưa có WAV sẽ không xuất hiện trong danh sách.

> 💡 **Save WAV vào đúng folder của chương tương ứng** — tool tự đọc `meta.json`
> để biết chương này là chương nào, không phụ thuộc vào tên file WAV.
""")
            with gr.Row():
                dd_folder   = gr.Dropdown(label="Chọn folder (có WAV)", choices=[], interactive=True, scale=4)
                btn_refresh = gr.Button("🔄 Refresh", scale=1)

            with gr.Row():
                btn_upload_one = gr.Button("🚀 Upload chương này", variant="primary")
                btn_upload_all = gr.Button("🚀 Upload tất cả", variant="secondary")

            out_upload = gr.Textbox(label="Log", lines=15, interactive=False)

            btn_refresh.click(refresh_upload_list, outputs=dd_folder)
            btn_upload_one.click(do_upload_one, inputs=dd_folder, outputs=out_upload)
            btn_upload_all.click(do_upload_all, outputs=out_upload)

        # ── Tab 3: Trạng thái ────────────────────────────────────────────────
        with gr.Tab("📊 Trạng thái"):
            gr.Markdown("Xem tất cả chương đã fetch và trạng thái của chúng.")
            btn_status = gr.Button("🔄 Refresh", variant="secondary")
            out_summary = gr.Markdown()
            out_table  = gr.Dataframe(
                headers=["ID", "Tiêu đề", "Trạng thái", "File WAV", "Folder"],
                datatype=["number", "str", "str", "str", "str"],
                interactive=False,
            )

            with gr.Row():
                dd_reset    = gr.Dropdown(label="Reset chương (về FAILED)", choices=[], interactive=True, scale=4)
                btn_reset_r = gr.Button("🔄 Refresh list", scale=1)
            btn_do_reset = gr.Button("⚠️ Reset chương này", variant="stop")
            out_reset    = gr.Markdown()

            btn_status.click(do_refresh_status, outputs=[out_table, out_summary])
            btn_reset_r.click(lambda: gr.update(choices=_all_folders()), outputs=dd_reset)
            btn_do_reset.click(do_reset, inputs=dd_reset, outputs=out_reset)

    # Auto-load status khi mở app
    demo.load(do_refresh_status, outputs=[out_table, out_summary])
    demo.load(refresh_upload_list, outputs=dd_folder)


if __name__ == "__main__":
    demo.launch(server_port=7861, inbrowser=True)
