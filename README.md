**Các file chính trong `script/`**
- `main.py` — entry tiện lợi cho pipeline mẫu. Thường gọi `genFullMap.create_sharded_traffic_map()` rồi `app.main()`.
- `genCluster.py` — gom nhóm (clustering) các điểm dừng từ `HCMC_bus_routes/*` bằng DBSCAN; hàm chính: `group_stops_nested_structure(root_folder, output_file, radius_meters)`; kết quả mặc định: `grouped_stops_nested.csv`.
- `genFullMap.py` — xử lý hàng loạt file GPS (thư mục `raw_GPS/`), thực hiện map-matching vào các node (cluster) và sinh các mảnh JSON trong `traffic_data_chunks/` (files: `YYYY-MM-DD_H.json`), đồng thời xuất `nodes.json` và `index.json`.
- `genCSV.py` — tổng hợp các mảnh trong `traffic_data_chunks/` thành CSV:
  - `traffic_nodes.csv` (danh sách node),
  - `traffic_edges_summary.csv` (tổng hợp cạnh/giá trị trung bình),
  - `traffic_trips_detailed.csv` (chi tiết theo vehicle/trips).
- `genMap.py` — tạo bản đồ tĩnh (`bus_nodes_static_map.png`) và trang HTML tương tác (`bus_network_interactive.html`) từ `grouped_stops_nested.csv`.
- `genPath.py` — xây dựng đồ thị di chuyển (NetworkX) từ `grouped_stops_nested.csv` và một file GPS mẫu; xuất `.gexf` (hàm: `build_graph_with_unified_radius(nodes_file, gps_file, output_file, UNIFIED_RADIUS)`).
- `app.py` — (tùy dự án) có thể chứa logic giao diện/visualization. `main.py` gọi `app.main()` ở cuối pipeline mẫu.
- `json2ndjson.py` — tiện ích chuyển đổi/chuẩn hóa JSON -> NDJSON (nếu cần).

**Phụ thuộc Python (khuyến nghị)**
- pandas
- numpy
- scipy
- scikit-learn
- networkx
- matplotlib

Cài đặt nhanh (PowerShell):

```powershell
pip install pandas numpy scipy scikit-learn networkx matplotlib
```

**Thứ tự chạy đề xuất (ví dụ thực tế)**
1) Tạo file `grouped_stops_nested.csv` (gom nhóm trạm):

```powershell
# từ thư mục script
python -c "import genCluster as gc; gc.group_stops_nested_structure('../HCMC_bus_routes', output_file='../grouped_stops_nested.csv', radius_meters=200)"
```

2) Tạo các mảnh traffic map (map-matching GPS vào node clusters):

```powershell
python -c "import genFullMap as gf; gf.create_sharded_traffic_map('../grouped_stops_nested.csv', '../raw_GPS', radius=200)"
```

3) Chuyển các mảnh thành CSV tổng hợp/chi tiết:

```powershell
python -c "import genCSV as gcsv; gcsv.export_data()"
```

4) (Tùy chọn) Tạo đồ thị di chuyển từ một file GPS mẫu:

```powershell
python -c "import genPath as gp; gp.build_graph_with_unified_radius('../grouped_stops_nested.csv', '../raw_GPS/anonymized_raw_2025-04-01.csv', output_file='traffic_graph.gexf', UNIFIED_RADIUS=200)"
```

5) (Tùy chọn) Sinh bản đồ tĩnh/HTML:

```powershell
python -c "import genMap as gm"  # genMap khi chạy sẽ đọc grouped_stops_nested.csv và tạo ảnh/HTML
```

6) (Tiện lợi) Chạy pipeline mẫu bằng `main.py`:

```powershell
python .\main.py
```

**Lưu ý vận hành & đường dẫn**
- Các script trong `script/` dùng nhiều đường dẫn tương đối (ví dụ: `../raw_GPS`, `../grouped_stops_nested.csv`). Chạy các lệnh từ thư mục `script/` để đảm bảo đường dẫn khớp.
- Tham số chính thường là bán kính (radius) khi gom nhóm hoặc map-matching; điều chỉnh theo chất lượng dữ liệu GPS.
- `genFullMap.create_sharded_traffic_map()` sẽ xóa `traffic_data_chunks/` nếu đã tồn tại rồi tạo lại — sao lưu nếu cần.

**Gợi ý debug nhanh**
- Lỗi thiếu module -> `pip install <package>`.
- Lỗi file không tìm thấy -> kiểm tra bạn đang `cd` vào `script/` trước khi chạy, hoặc chỉnh đường dẫn file.
- Nếu dữ liệu GPS có timestamp không chuẩn, kiểm tra cột `datetime` trong file CSV đầu vào.
