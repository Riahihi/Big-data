"""Cấu hình chung: địa điểm, thời gian, các biến cần lấy, đường dẫn thư mục"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RAW_DIR = DATA / "raw"  # JSON gốc (gzip) đúng như API trả về
RAW_DAILY_DIR = DATA / "raw_daily"  # daily API dùng để đối chiếu độc lập
MANIFEST = DATA / "manifest.jsonl"  # mỗi dòng = một lần tải thành công
CLEAN_DIR = DATA / "clean"  # Parquet đã làm sạch, partition theo location_id/year
RESULTS_DIR = DATA / "results"  # các bảng kết quả (Parquet + CSV)
REPORTS = ROOT / "reports"
FIGURES = REPORTS / "figures"
WEB_DATA = ROOT / "web" / "static" / "data"

API_URL = "https://archive-api.open-meteo.com/v1/archive"
TIMEZONE = "Asia/Ho_Chi_Minh"  # UTC+7, Việt Nam không có giờ mùa hè
MODEL = "era5"  # cố định một mô hình để chuỗi thời gian đồng nhất (xem README)

START_YEAR, END_YEAR = 2016, 2025  # phạm vi mở rộng
CORE_START, CORE_END = 2019, 2023  # phạm vi bắt buộc theo đề

HOURLY_VARS = [
    "temperature_2m",  # °C
    "relative_humidity_2m",  # %
    "dew_point_2m",  # °C
    "apparent_temperature",  # °C
    "precipitation",  # mm, tổng của giờ TRƯỚC thời điểm ghi (mưa + tuyết)
    "rain",  # mm
    "weather_code",  # mã WMO
    "cloud_cover",  # %
    "wind_speed_10m",  # km/h
    "wind_gusts_10m",  # km/h
]

# 34 tỉnh/thành (sau sáp nhập 7/2025), tọa độ lấy ở trung tâm hành chính
# cột cuối = True là 10 điểm theo đề
LOCATIONS = [
    # id, tên tỉnh/thành, điểm lấy mẫu (đô thị), vĩ độ, kinh độ, vùng, core
    ("HNI", "Hà Nội", "Hà Nội", 21.0285, 105.8542, "Đồng bằng sông Hồng", True),
    ("HPG", "Hải Phòng", "Hải Phòng", 20.8449, 106.6881, "Đồng bằng sông Hồng", False),
    ("HYN", "Hưng Yên", "Hưng Yên", 20.6464, 106.0511, "Đồng bằng sông Hồng", False),
    ("NBH", "Ninh Bình", "Ninh Bình", 20.2506, 105.9745, "Đồng bằng sông Hồng", False),
    ("BNH", "Bắc Ninh", "Bắc Giang", 21.2731, 106.1946, "Đồng bằng sông Hồng", False),
    ("QNH", "Quảng Ninh", "Hạ Long", 20.9517, 107.0800, "Đông Bắc", True),
    ("LSN", "Lạng Sơn", "Lạng Sơn", 21.8537, 106.7615, "Đông Bắc", False),
    ("CBG", "Cao Bằng", "Cao Bằng", 22.6657, 106.2577, "Đông Bắc", False),
    ("TNN", "Thái Nguyên", "Thái Nguyên", 21.5942, 105.8482, "Đông Bắc", False),
    ("TQG", "Tuyên Quang", "Tuyên Quang", 21.8236, 105.2140, "Đông Bắc", False),
    ("PTO", "Phú Thọ", "Việt Trì", 21.3227, 105.4019, "Đông Bắc", False),
    ("LCI", "Lào Cai", "Yên Bái", 21.7229, 104.9113, "Tây Bắc", False),
    ("DBN", "Điện Biên", "Điện Biên Phủ", 21.3860, 103.0230, "Tây Bắc", True),
    ("LCU", "Lai Châu", "Lai Châu", 22.3964, 103.4582, "Tây Bắc", False),
    ("SLA", "Sơn La", "Sơn La", 21.3256, 103.9188, "Tây Bắc", False),
    ("THA", "Thanh Hóa", "Thanh Hóa", 19.8067, 105.7852, "Bắc Trung Bộ", False),
    ("NAN", "Nghệ An", "Vinh", 18.6796, 105.6813, "Bắc Trung Bộ", True),
    ("HTH", "Hà Tĩnh", "Hà Tĩnh", 18.3428, 105.9057, "Bắc Trung Bộ", False),
    ("QTI", "Quảng Trị", "Đồng Hới", 17.4689, 106.6223, "Bắc Trung Bộ", False),
    ("HUE", "Huế", "Huế", 16.4637, 107.5909, "Bắc Trung Bộ", True),
    ("DNG", "Đà Nẵng", "Đà Nẵng", 16.0544, 108.2022, "Nam Trung Bộ", True),
    ("QNG", "Quảng Ngãi", "Quảng Ngãi", 15.1214, 108.8044, "Nam Trung Bộ", False),
    ("GLI", "Gia Lai", "Quy Nhơn", 13.7765, 109.2237, "Nam Trung Bộ", False),
    ("KHA", "Khánh Hòa", "Nha Trang", 12.2388, 109.1967, "Nam Trung Bộ", True),
    ("DLK", "Đắk Lắk", "Buôn Ma Thuột", 12.6667, 108.0500, "Tây Nguyên", False),
    ("LDG", "Lâm Đồng", "Đà Lạt", 11.9404, 108.4583, "Tây Nguyên", True),
    ("HCM", "TP. Hồ Chí Minh", "TP. Hồ Chí Minh", 10.7769, 106.7009, "Đông Nam Bộ", True),
    ("DNI", "Đồng Nai", "Biên Hòa", 10.9574, 106.8426, "Đông Nam Bộ", False),
    ("TNH", "Tây Ninh", "Tân An", 10.5359, 106.4137, "Đông Nam Bộ", False),
    ("CTO", "Cần Thơ", "Cần Thơ", 10.0452, 105.7469, "Đồng bằng sông Cửu Long", True),
    ("VLG", "Vĩnh Long", "Vĩnh Long", 10.2537, 105.9722, "Đồng bằng sông Cửu Long", False),
    ("DTP", "Đồng Tháp", "Mỹ Tho", 10.3600, 106.3600, "Đồng bằng sông Cửu Long", False),
    ("AGG", "An Giang", "Rạch Giá", 10.0124, 105.0809, "Đồng bằng sông Cửu Long", False),
    ("CMU", "Cà Mau", "Cà Mau", 9.1769, 105.1524, "Đồng bằng sông Cửu Long", False),
]

LOCATION_FIELDS = ["location_id", "location_name", "site_name", "latitude", "longitude", "region", "is_core"]


def locations_as_dicts():
    return [dict(zip(LOCATION_FIELDS, row)) for row in LOCATIONS]


def core_ids():
    return [r[0] for r in LOCATIONS if r[6]]
