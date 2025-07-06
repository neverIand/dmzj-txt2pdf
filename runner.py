from dmzj_txt2pdf import convert_dmzj_txts_to_pdf

pdfs = convert_dmzj_txts_to_pdf(
    root_dir=r"C:\Users\90833\AppData\Roaming\com.xycz\flutter_dmzj\novel",
    output_dir=r"D:\DMZJ_PDF",
    group_level=2,
)
print("PDFs written:", pdfs)
