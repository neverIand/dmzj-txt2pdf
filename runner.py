from dmzj_txt2pdf import convert_dmzj_txts_to_pdf

pdfs = convert_dmzj_txts_to_pdf(
    root_dir=r"C:\Users\90833\AppData\Roaming\com.xycz\flutter_dmzj\novel",
    output_dir=r"D:\DMZJ_PDF",
    group_level=1,         # 1 = whole novel, 2 = volume, 3 = no merge
    encoding="gbk",      # change to "gbk" if you see mojibake
)
print("PDFs written:", pdfs)