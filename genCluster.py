import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
import os

def group_stops_nested_structure(root_folder, output_file='grouped_stops_nested.csv', radius_meters=200):
    print(f"Đang quét cấu trúc thư mục tại: {root_folder}...")
    
    df_list = []
    
    # 1. Duyệt qua tất cả thư mục con bằng os.walk
    # os.walk sẽ đi vào từng ngóc ngách của thư mục root_folder
    for current_root, dirs, files in os.walk(root_folder):
        
        # Lấy tên thư mục hiện tại làm RouteID (ví dụ: thư mục "01" -> RouteID = "01")
        route_id = os.path.basename(current_root)
        
        # Bỏ qua thư mục gốc nếu nó không chứa file dữ liệu
        if current_root == root_folder:
            continue

        # Định nghĩa 2 file cần tìm
        file_map = {
            'stops_by_var.csv': 'Luot_Di',    # Gán nhãn hướng đi
            'rev_stops_by_var.csv': 'Luot_Ve' # Gán nhãn hướng về
        }

        for filename, direction in file_map.items():
            if filename in files:
                file_path = os.path.join(current_root, filename)
                try:
                    # Đọc file CSV
                    df = pd.read_csv(file_path, skipinitialspace=True)
                    df.columns = df.columns.str.strip() # Chuẩn hóa tên cột
                    
                    # Chọn các cột cần thiết
                    cols_needed = ['StopId', 'Code', 'Name', 'Lat', 'Lng', 'StopType', 'Street', 'Routes']
                    available_cols = [c for c in cols_needed if c in df.columns]
                    temp_df = df[available_cols].copy()
                    
                    # --- QUAN TRỌNG: Thêm thông tin ngữ cảnh ---
                    temp_df['RouteId'] = route_id   # Lấy từ tên thư mục
                    temp_df['Direction'] = direction # Lấy từ tên file
                    
                    df_list.append(temp_df)
                    
                except Exception as e:
                    print(f"Lỗi đọc file {file_path}: {e}")

    if not df_list:
        print("Không tìm thấy dữ liệu nào trong các thư mục con!")
        return

    # 2. Gộp tất cả thành một bảng Master
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df = combined_df.dropna(subset=['Lat', 'Lng'])

    print(f"Tổng số trạm tìm thấy từ {len(df_list)} file con: {len(combined_df)}")
    print("Đang chạy thuật toán gom nhóm vị trí (DBSCAN)...")

    # 3. Chuẩn bị DBSCAN
    coords = np.radians(combined_df[['Lat', 'Lng']].values)
    kms_per_radian = 6371.0088
    epsilon = (radius_meters / 1000) / kms_per_radian

    # 4. Chạy DBSCAN
    db = DBSCAN(eps=epsilon, min_samples=1, metric='haversine', algorithm='ball_tree').fit(coords)
    combined_df['cluster_label'] = db.labels_

    # 5. Tính tọa độ trung tâm
    centroids = combined_df.groupby('cluster_label')[['Lat', 'Lng']].mean().reset_index()
    centroids.columns = ['cluster_label', 'centroid_lat', 'centroid_lng']

    # 6. Merge kết quả
    final_df = pd.merge(combined_df, centroids, on='cluster_label')

    # Sắp xếp lại cột cho đẹp
    cols_order = ['cluster_label', 'centroid_lat', 'centroid_lng', 'RouteId', 'Direction', 'Name', 'Code', 'Lat', 'Lng', 'Routes']
    # Chỉ chọn các cột có tồn tại trong dataframe
    cols_order = [c for c in cols_order if c in final_df.columns]
    
    final_df = final_df[cols_order]

    # 7. Xuất file
    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"Hoàn tất! Đã lưu file tổng hợp tại: {output_file}")
    print(f"Số điểm dừng duy nhất (Cluster): {len(centroids)}")
    return output_file

# --- CÁCH CHẠY ---
# Giả sử thư mục gốc của bạn tên là "All_Bus_Data"
