# dmzj-txt2pdf
Command line:
```shell
python dmzj_txt2pdf.py \
       --root "C:\\Users\\[$USER]\\AppData\\Roaming\\com.xycz\\flutter_dmzj\\novel" \
       --out  "D:\\DMZJ_PDF" \
       --group-level 2
# ⇒ Only 3084_11641.pdf, 3084_11642.pdf … remain; chapter PDFs vanish.

```
Call in python code:

```python
from dmzj_txt2pdf import convert_dmzj_txts_to_pdf

pdfs = convert_dmzj_txts_to_pdf(
    root_dir=r"C:\Users\[$USER]\AppData\Roaming\com.xycz\flutter_dmzj\novel",
    output_dir=r"D:\DMZJ_PDF",
    group_level=2,  # 1 = whole novel, 2 = volume, 3 = no merge
)
print("PDFs written:", pdfs)
```

Credit:
ChatGPT o3


