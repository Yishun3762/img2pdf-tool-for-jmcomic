import os
import time
import tempfile
import shutil
from PIL import Image
import zipfile
import gc

# 尝试导入 img2pdf，如果失败则标记为不可用
try:
    import img2pdf
    IMG2PDF_AVAILABLE = True
except ImportError:
    IMG2PDF_AVAILABLE = False
    print("警告: img2pdf 库未安装，将仅使用 PIL 进行 PDF 转换。建议安装以提高性能：pip install img2pdf")

# 批次大小常量 - 可以根据实际情况调整
BATCH_SIZE = 10  # 每批处理的图片数

# --- 辅助函数：检查图片格式 ---
def get_image_formats(image_dir, image_files):
    formats = set()
    unsupported_by_img2pdf = False
    for filename in image_files:
        ext = os.path.splitext(filename)[1].lower()
        formats.add(ext)
        if ext not in ('.jpg', '.jpeg', '.png'): # img2pdf 主要支持这些
            unsupported_by_img2pdf = True
    return formats, unsupported_by_img2pdf

def images_to_pdf(image_dir, output_pdf_path):
    """
    将指定文件夹内的所有图片按顺序合并为PDF文件。
    优先使用 img2pdf (如果可用且格式支持)，否则回退到优化的 PIL 算法。
    """
    start_time = time.time()
    allowed_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.bmp'}
    
    try:
        image_files = []
        try:
            with os.scandir(image_dir) as entries:
                for entry in entries:
                    if entry.is_file():
                        ext = os.path.splitext(entry.name)[1].lower()
                        if ext in allowed_extensions:
                            image_files.append(entry.name)
        except FileNotFoundError:
            print(f"错误：目录不存在 '{image_dir}'")
            return
        
        try:
            image_files.sort(key=lambda x: int(os.path.splitext(x)[0]))
        except ValueError:
            image_files.sort()
            
        if not image_files:
            print(f"警告：目录 '{image_dir}' 中未找到任何图片文件")
            return

        os.makedirs(os.path.dirname(os.path.abspath(output_pdf_path)), exist_ok=True)
        
        # --- 策略选择 ---
        _, contains_unsupported_for_img2pdf = get_image_formats(image_dir, image_files)
        
        use_img2pdf_strategy = IMG2PDF_AVAILABLE and not contains_unsupported_for_img2pdf
        
        if use_img2pdf_strategy:
            print(f"信息：为目录 '{image_dir}' 使用 img2pdf策略 (所有格式兼容)")
            try:
                img_paths = [os.path.join(image_dir, fname) for fname in image_files]
                with open(output_pdf_path, "wb") as f:
                    f.write(img2pdf.convert(img_paths))
                print(f"成功使用 img2pdf 生成PDF：'{output_pdf_path}' (共 {len(image_files)} 张图片，耗时 {time.time() - start_time:.2f} 秒)")
                return output_pdf_path
            except Exception as img2pdf_err:
                print(f"警告：img2pdf 转换失败 ('{image_dir}'): {img2pdf_err}。将回退到 PIL 算法。")
                # img2pdf 失败，则强制使用 PIL
                use_img2pdf_strategy = False 
        
        # --- PIL 分批处理算法 (如果 img2pdf 不适用或失败) ---
        if not use_img2pdf_strategy:
            print(f"信息：为目录 '{image_dir}' 使用 PIL 分批处理策略")
            temp_pdfs = []
            with tempfile.TemporaryDirectory() as temp_dir:
                total_images = len(image_files)
                batch_count = (total_images + BATCH_SIZE - 1) // BATCH_SIZE
                print(f"共有 {total_images} 张图片，分 {batch_count} 批处理 (PIL)")
                
                for batch_index in range(batch_count):
                    start_idx = batch_index * BATCH_SIZE
                    end_idx = min(start_idx + BATCH_SIZE, total_images)
                    batch_files_pil = image_files[start_idx:end_idx]
                    print(f"PIL处理批次 {batch_index+1}/{batch_count} (图片 {start_idx+1}-{end_idx})")
                    
                    batch_images_pil = []
                    for filename in batch_files_pil:
                        img_path = os.path.join(image_dir, filename)
                        try:
                            img = Image.open(img_path)
                            if img.mode != 'RGB':
                                img = img.convert('RGB')
                            batch_images_pil.append(img)
                        except Exception as e:
                            print(f"警告(PIL)：无法处理文件 '{img_path}'，已跳过。错误信息：{str(e)}")
                            continue
                    
                    if not batch_images_pil:
                        print(f"警告(PIL)：批次 {batch_index+1} 中没有有效图片，跳过")
                        continue
                    
                    temp_pdf_path = os.path.join(temp_dir, f"temp_batch_{batch_index+1}.pdf")
                    try:
                        batch_images_pil[0].save(
                            temp_pdf_path, "PDF", save_all=True, 
                            append_images=batch_images_pil[1:], optimize=True
                        )
                        temp_pdfs.append(temp_pdf_path)
                    except Exception as e:
                        print(f"PIL批次 {batch_index+1} PDF生成错误：{str(e)}")
                    
                    for img in batch_images_pil:
                        try: img.close()
                        except: pass
                    batch_images_pil.clear()
                    gc.collect()
                
                if temp_pdfs:
                    if len(temp_pdfs) == 1:
                        shutil.copy2(temp_pdfs[0], output_pdf_path)
                        print(f"成功使用PIL生成PDF：'{output_pdf_path}' (单批次)")
                    else:
                        if merge_pdfs(temp_pdfs, output_pdf_path):
                             print(f"成功使用PIL生成PDF：'{output_pdf_path}' (合并 {len(temp_pdfs)} 个批次)")
                        else:
                             print(f"错误(PIL): 合并临时PDF失败 for '{output_pdf_path}'")
                             return None # 合并失败则返回 None
                else:
                    print(f"错误(PIL)：未能生成任何临时PDF文件 for '{image_dir}'")
                    return None

    except Exception as e:
        print(f"生成PDF时发生未处理的错误 ('{image_dir}'): {str(e)}")
        return None
    finally:
        gc.collect()
    
    print(f"PDF处理完成 for '{image_dir}'，总耗时 {time.time() - start_time:.2f} 秒")
    return output_pdf_path


def merge_pdfs(pdf_paths, output_path):
    """
    合并多个PDF文件为一个文件，使用低内存方式。
    返回 True 表示成功, False 表示失败。
    """
    try:
        from PyPDF2 import PdfMerger
        merger = PdfMerger()
        for pdf in pdf_paths:
            if os.path.exists(pdf) and os.path.getsize(pdf) > 0:
                merger.append(pdf)
            else:
                print(f"警告 (merge_pdfs): 跳过无效的临时PDF '{pdf}'")
        if not merger.inputs: # PyPDF2 3.x.x merger.inputs / Older versions might need other checks
            print(f"错误 (merge_pdfs): 没有有效的PDF可供合并 for {output_path}")
            merger.close()
            return False
        merger.write(output_path)
        merger.close()
        return True
    except ImportError:
        print("错误 (merge_pdfs): PyPDF2 未安装，无法合并PDF。请安装 pip install PyPDF2")
        return False # PyPDF2 是首选方案，没有它则标记为失败
    except Exception as e:
        print(f"错误 (merge_pdfs): 合并PDF时出错 for '{output_path}': {e}")
        return False


def batch_chapter_to_pdfs(album_dir):
    """
    将专辑目录中的所有章节转换为PDF
    
    Args:
        album_dir: 专辑目录，包含多个章节子目录
        
    Returns:
        成功生成的PDF文件路径列表
    """
    print(f"批量处理目录: {album_dir}")
    pdf_paths = []
    
    try:
        # 获取子目录并尝试按数字排序
        chapters = []
        with os.scandir(album_dir) as entries:
            for entry in entries:
                if entry.is_dir():
                    chapters.append(entry.name)
        
        # 尝试数字排序，失败则按字母排序
        try:
            chapters.sort(key=int)
        except ValueError:
            chapters.sort()
        
        # 释放内存
        gc.collect()
        
        # 处理每个章节
        for chapter in chapters:
            chapter_dir = os.path.join(album_dir, chapter)
            pdf_path = os.path.join(album_dir, f"{chapter}.pdf")
            
            print(f"处理章节: {chapter}")
            result = images_to_pdf(chapter_dir, pdf_path)
            
            if result and os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
                pdf_paths.append(pdf_path)
            else:
                print(f"警告: 章节 '{chapter}' 处理失败")
            
            # 每个章节处理后清理内存
            gc.collect()
    
    except Exception as e:
        print(f"批量处理章节时发生错误: {str(e)}")
    
    return pdf_paths


def zip_pdfs(pdf_paths, zip_path):
    """
    将多个PDF文件打包为ZIP文件
    
    Args:
        pdf_paths: PDF文件路径列表
        zip_path: 输出的ZIP文件路径
    """
    if not pdf_paths:
        print("警告: 没有PDF文件可供打包")
        return
    
    try:
        # 确保输出目录存在
        os.makedirs(os.path.dirname(os.path.abspath(zip_path)), exist_ok=True)
        
        start_time = time.time()
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
            for pdf in pdf_paths:
                if os.path.exists(pdf) and os.path.getsize(pdf) > 0:
                    zipf.write(pdf, arcname=os.path.basename(pdf))
                else:
                    print(f"警告: 跳过无效的PDF文件 '{pdf}'")
        
        print(f"成功生成ZIP文件: '{zip_path}' (包含 {len(pdf_paths)} 个PDF，耗时 {time.time() - start_time:.2f} 秒)")
        return zip_path
    except Exception as e:
        print(f"生成ZIP文件时发生错误: {str(e)}")
        return None
