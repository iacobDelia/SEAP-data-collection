import os
import sys
import gc
import site

# configure cuda path and other dependencies
def setup_cuda_gpu():
    # find cuda path
    cuda_base = os.environ.get('CUDA_PATH') 
    
    if not cuda_base:
        print("ERROR with cuda toolkit")
        sys.exit(1)
        
    cuda_bin = os.path.join(cuda_base, "bin")
    
    # find nvidia-cudnn
    cudnn_bin = None
    try:
        import nvidia.cudnn as cudnn
        # find bin folder installed via pip
        cudnn_bin = os.path.join(os.path.dirname(cudnn.__file__), "bin")
    except ImportError:
        print("error with nvidia-cudnn")

    # modify PATH for this run
    paths_to_add = [cuda_bin]
    if cudnn_bin:
        paths_to_add.append(cudnn_bin)
        
    for path in paths_to_add:
        if os.path.exists(path):
            os.environ["PATH"] = path + os.pathsep + os.environ["PATH"]
            if hasattr(os, "add_dll_directory"):
                os.add_dll_directory(path)
            #print(f"added to session: {path}")

setup_cuda_gpu()

os.environ['PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK'] = 'True'

from paddleocr import PaddleOCR
import paddle

_ocr_engine = None

# load engine only once
def get_ocr_engine():
    global _ocr_engine
    if _ocr_engine is None:
        _ocr_engine = PaddleOCR(use_gpu=True, lang='ro', use_angle_cls=True, show_log=False)
    return _ocr_engine

#print(f"running on: {paddle.device.cuda.get_device_name()}")
def extract_text_from_scanned_pdf(path):
    try:
        # start on GPU
        ocr = get_ocr_engine()
        result = ocr.ocr(path, cls=True)
        all_pages_text = []
        
        #with open(output_file, 'w', encoding='utf-8') as f:
        for i, page in enumerate(result):
            if page:
                lines = [line[1][0] for line in page]
                page_content = f"PAGINA {i+1}\n" + "\n".join(lines)
                all_pages_text.append(page_content)
                #print(f"page {i+1} processed")
            gc.collect()
        return "\n\n".join(all_pages_text)
    except Exception as e:
        print(f"exception during OCR extraction: {e}")