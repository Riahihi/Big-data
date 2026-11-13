# Nhóm Đồ Án BigData
Thành viên nhóm:
- Phan Duy Minh Thắng - Mã SV: 21T1020694
- Lê Anh Quân - Mã SV: 22T1020710
- Lê Trần Khánh Tùng - Mã SV: 21T1020819
- Đậu Hà Văn - Mã SV: 21T1020826

# Đồ án Big Data: phân tích nhiệt độ và lượng mưa ở Việt Nam bằng PySpark

Pipeline lấy dữ liệu thời tiết theo giờ từ Open-Meteo, làm sạch và phân tích bằng PySpark, lưu kết quả ra Parquet,
vẽ biểu đồ và làm 1 trang web để xem kết quả.

Phạm vi theo đề là 10 địa điểm, 2019-2023. Nhóm lấy rộng hơn để làm thêm phần mở rộng:
34 tỉnh/thành (theo đơn vị hành chính mới từ 7/2025), 2016-2025, 10 biến (nhiệt độ, mưa, độ ẩm, điểm sương,
nhiệt độ cảm nhận, mây, gió, gió giật, mã thời tiết). Tổng cộng 2.980.848 bản ghi theo giờ.

## Cài đặt

Cần Python 3.12 và Java 17.

```bash
cd weather_bigdata
```

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Linux/macOS:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Chạy

Repo đã có sẵn dữ liệu gốc (`data/raw`) và các bảng kết quả (`data/results`), nhưng không có dữ liệu sạch
(`data/clean`, khoảng 50 MB). Trước khi mở web cần chạy `python run_pipeline.py --skip-ingest` một lần (khoảng 5 phút).

Trên Windows, `pipeline/spark_utils.py` tự tải adapter Java NIO đã ghim SHA-256 để Spark đọc/ghi
file cục bộ mà không cần `winutils.exe`; lần chạy đầu cần truy cập Maven Central. Kích hoạt môi
trường ảo bằng `.venv\Scripts\Activate.ps1` trong PowerShell.

```bash
python run_pipeline.py                 # chạy hết
python run_pipeline.py --skip-ingest   # không tải lại dữ liệu
python run_pipeline.py --only verify   # chạy 1 bước
```

Hoặc chạy từng file:

```bash
python pipeline/ingest.py          # tải dữ liệu
python pipeline/ingest.py --daily-check
python pipeline/clean.py           # làm sạch
python pipeline/analysis.py        # 3 câu hỏi
python pipeline/advanced.py        # phần mở rộng
python pipeline/verify.py          # kiểm tra kết quả
python pipeline/benchmark.py       # đo hiệu năng
python pipeline/figures.py         # vẽ biểu đồ
python pipeline/checkpoint_report.py # tạo báo cáo sau khi chạy đủ pipeline
python reports/build_checkpoint3_pdf.py # xuất reports/checkpoint3.pdf
python -m pytest tests -q          # test
python web/app.py                  # web, mở http://127.0.0.1:7890
```

Lưu ý: API miễn phí của Open-Meteo bị giới hạn số lần gọi mỗi giờ, nên tải hết 340 file mất khoảng 1 giờ
(script tự chờ khi bị chặn). Chạy lại thì các file đã tải sẽ được bỏ qua.

## Cấu trúc

```
pipeline/
  config.py        danh sách địa điểm, thời gian, các biến
  spark_utils.py   tạo SparkSession
  ingest.py        tải dữ liệu, lưu file gốc + manifest
  clean.py         làm sạch, ghi parquet
  analysis.py      câu 1, 2, 3
  advanced.py      phần mở rộng
  verify.py        kiểm tra kết quả
  benchmark.py     đo hiệu năng (mức B)
  figures.py       vẽ biểu đồ
tests/             test trên dữ liệu giả
web/               web demo (FastAPI)
data/raw/          file json gốc
data/clean/        dữ liệu sạch (parquet)
data/results/      các bảng kết quả (parquet + csv)
reports/figures/   biểu đồ png
reports/evidence/  Spark formatted plan
reports/checkpoint3.md  báo cáo/evidence Checkpoint 3
reports/checkpoint3.pdf  bản PDF theo mẫu Checkpoint 3
```

`pipeline/clean.py` lưu execution plan ở `reports/evidence/clean_plan.txt` và dừng nếu join
grid/dimension làm mất hoặc nhân bản dữ liệu. Báo cáo Checkpoint 3 được tạo ở bước cuối của
`run_pipeline.py`; bước report từ chối tạo báo cáo nếu clean, analysis, advanced, verify,
benchmark hoặc figures chưa thành công.

## Dữ liệu

- Nguồn: Open-Meteo Historical Weather API, chọn mô hình `era5` (dữ liệu tái phân tích của ECMWF, ô lưới khoảng 28 km,
  không phải số đo ở trạm). Nhóm cố định 1 mô hình để số liệu các năm đồng nhất với nhau.
- Múi giờ Asia/Ho_Chi_Minh.
- Mỗi lần tải được ghi vào `data/manifest.jsonl` (url, số bản ghi, sha256, thời gian tải).

## Làm sạch

- R1: đổi kiểu dữ liệu, parse thời gian
- R2: bỏ dòng trùng (location_id, ts)
- R3: tạo đủ 24 giờ mỗi ngày rồi join để tìm giờ bị thiếu
- R4: giá trị ngoài ngưỡng thì gán null
- R5: thiếu tối đa 3 giờ thì nội suy (chỉ với nhiệt độ, độ ẩm..., không nội suy mưa)
- R6: giá trị mưa lúc H là mưa của 1 giờ trước đó, nên mưa ngày D lấy từ 01:00 ngày D đến 00:00 ngày D+1

Dữ liệu tải về không bị thiếu hay trùng, nên các bước R2-R5 được kiểm tra bằng test với dữ liệu giả.

Về R6: lúc đầu nhóm cộng mưa từ 0h đến 23h, khi so với dữ liệu ngày của Open-Meteo thì chỉ khớp khoảng 79%.
Kiểm tra lại mới thấy giá trị lúc 00:00 là mưa của giờ cuối ngày hôm trước. Sửa lại thì khớp 100%.

## Ba câu hỏi

- Câu 1: nhiệt độ TB tháng = trung bình các giá trị theo giờ trong tháng (`q1_monthly_temp_core.csv`)
- Câu 2: mưa giờ cộng thành ngày, ngày cộng thành tháng (`q2_monthly_precip_core.csv`)
- Câu 3: top 5 ngày mưa lớn nhất mỗi nơi, chỉ tính ngày đủ 24h, dùng `rank()` nên nếu bằng nhau ở hạng 5 thì lấy
  cả (`q3_top5_rain_days_core.csv`)

File `_core` là theo đề (10 điểm, 2019-2023), file `_full` là 34 tỉnh, 2016-2025.

## Phần mở rộng

Chỉ số cực trị theo năm, các đợt nắng nóng, chuỗi ngày khô, số ngày theo cấp mưa, xu hướng nhiệt độ,
mưa và nhiệt độ theo giờ trong ngày, chu kỳ lặp lại của mưa lớn (Gumbel), các đợt mưa lớn 3 ngày,
và phân nhóm khí hậu các tỉnh bằng KMeans.

## Kiểm tra

- Tính lại 3 câu hỏi bằng pandas (đọc thẳng file json, không qua spark) rồi so sánh: khớp hết.
- So với dữ liệu ngày của Open-Meteo (18.260 ngày, 10 điểm): mưa khớp 100%, nhiệt độ TB khớp 100%.
- Cộng tay 24 giờ của ngày 12/09/2021 ở Đà Nẵng: 200,1 mm, giống kết quả Spark.
- Đọc lại parquet đủ 2.980.848 dòng.
- 14 test trong `tests/` đều chạy qua.

## Một số kết quả

- Ngày mưa lớn nhất (2019-2023): Đà Nẵng 12/09/2021 (200,1 mm), Nghệ An 18/10/2020 (162,6 mm),
  Huế 11/11/2020 (157,5 mm), Hà Nội 03/08/2019 (155,7 mm).
- Đợt mưa 3 ngày lớn nhất: Hà Tĩnh 17-19/10/2020 với 865 mm (đợt lũ miền Trung tháng 10/2020).
  Bão Yagi cũng thấy rõ: ngày 07/09/2024 Hưng Yên mưa 230,6 mm, Hà Nội 208,6 mm.
- Năm 2024 và 2019 nóng nhất trong 10 năm. Xu hướng tăng nhiệt chưa rõ, chỉ 4/34 điểm có ý nghĩa thống kê,
  10 năm là chưa đủ để kết luận.
- KMeans chia được 4 nhóm khá giống các vùng khí hậu: miền Bắc, duyên hải miền Trung, miền Nam và Đà Lạt.

Hiệu năng (đo trên server dùng chung nên chỉ để so sánh tương đối):

| | Kết quả |
|---|---|
| Dung lượng | CSV 611 MB, Parquet 37 MB |
| Đọc và tính câu 1 | CSV 2,47 s, Parquet 0,36 s |
| Phân vùng | đọc 0,14 MB thay vì 70,7 MB nhưng không nhanh hơn vì có nhiều file nhỏ |
| 1 luồng lên 8 luồng | nhanh hơn khoảng 2,5-3 lần |
| So với pandas | Spark nhanh hơn khoảng 1,7-2 lần |

## Hạn chế

- ERA5 là dữ liệu ô lưới nên mưa lớn và nhiệt độ cao nhất thường thấp hơn số đo ở trạm.
- 10 năm là ngắn để nói về xu hướng, và chu kỳ lặp lại trên 10 năm chỉ để tham khảo.
- Tháng 12/2025 bị đánh dấu thiếu vì ngày 31/12/2025 thiếu giá trị mưa lúc 00:00 ngày 1/1/2026.
- Benchmark đo trên máy dùng chung nên số liệu có thể dao động.
