import pandas as pd
import numpy as np
import json
import os
import glob
import re
import shutil
from scipy.spatial import cKDTree

# --- CẤU HÌNH ---
OUTPUT_DIR = 'traffic_data_chunks'

# Class hỗ trợ convert số numpy sang số python tự động
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

def haversine_np(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371000 * c

def create_sharded_traffic_map(
    nodes_file: str,
    gps_folder: str,
    radius: int = 50,
    max_time: int = 5400,
    min_time: int = 5
):
    print(f"\n--- [TrafficMap Sharding] Bắt đầu xử lý Big Data ---")
    
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)
    print(f"✔ Đã tạo thư mục output: {OUTPUT_DIR}")

    # 1. LOAD NODES
    print("1. Đang đọc dữ liệu Node...")
    try:
        nodes_df = pd.read_csv(nodes_file)
        unique_nodes = nodes_df.groupby('cluster_label').agg({
            'centroid_lat': 'first', 'centroid_lng': 'first', 'Name': 'first'
        }).reset_index()
        
        nodes_meta = {}
        for _, row in unique_nodes.iterrows():
            # Ép kiểu int() cho Key để tránh lỗi
            nodes_meta[int(row['cluster_label'])] = {
                "lat": float(row['centroid_lat']),
                "lng": float(row['centroid_lng']),
                "name": str(row['Name'])
            }
            
        # Lưu file nodes.json
        with open(os.path.join(OUTPUT_DIR, 'nodes.json'), 'w', encoding='utf-8') as f:
            json.dump(nodes_meta, f, cls=NpEncoder) # Dùng NpEncoder

        # KDTree Setup
        R = 6371000
        phi = np.radians(unique_nodes['centroid_lat'])
        theta = np.radians(unique_nodes['centroid_lng'])
        tree_data = np.column_stack((
            R * np.cos(phi) * np.cos(theta),
            R * np.cos(phi) * np.sin(theta),
            R * np.sin(phi)
        ))
        tree = cKDTree(tree_data)
        idx_to_label = unique_nodes['cluster_label'].to_dict()
        
    except Exception as e:
        print(f"❌ Node Error: {e}")
        return

    # 2. PROCESS FILES & SPLIT
    search_path = os.path.join(gps_folder, "*.csv")
    gps_files = sorted(glob.glob(search_path))
    
    index_data = {} 
    print(f"2. Đang xử lý {len(gps_files)} file GPS...")

    for file_path in gps_files:
        filename = os.path.basename(file_path)
        match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        if not match: continue
        date_key = match.group(1)
        
        try:
            df = pd.read_csv(file_path)
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            # Map Matching
            phi_gps = np.radians(df['lat'])
            theta_gps = np.radians(df['lng'])
            gx = R * np.cos(phi_gps) * np.cos(theta_gps)
            gy = R * np.cos(phi_gps) * np.sin(theta_gps)
            gz = R * np.sin(phi_gps)
            
            dists, idxs = tree.query(np.column_stack((gx, gy, gz)), k=1, distance_upper_bound=radius)
            valid = idxs < len(unique_nodes)
            df['node_id'] = -1
            df.loc[valid, 'node_id'] = np.array([idx_to_label[i] for i in idxs[valid]])
            
            df = df.sort_values(['anonymized_vehicle', 'datetime'])
            
            hourly_data = {} 
            agg_store = {} 

            for vid, group in df.groupby('anonymized_vehicle'):
                vid_str = str(vid)
                node_ids = group['node_id'].values
                times = group['datetime'].values
                
                last_node = -1
                departure_time = None
                
                for i in range(len(node_ids)):
                    curr_node = node_ids[i]
                    curr_time = times[i]
                    
                    if curr_node != -1:
                        if last_node != -1 and last_node != curr_node:
                            if departure_time is not None:
                                travel_time = (curr_time - departure_time) / np.timedelta64(1, 's')
                                if min_time < travel_time < max_time:
                                    dt_obj = pd.to_datetime(departure_time)
                                    h = str(dt_obj.hour)
                                    
                                    if h not in hourly_data: hourly_data[h] = {"vehicles": {}, "aggregated": []}
                                    if h not in agg_store: agg_store[h] = {}

                                    # Lấy tọa độ để tính vận tốc
                                    # Ép kiểu int() cho last_node, curr_node khi truy cập dict
                                    u_info = nodes_meta[int(last_node)]
                                    v_info = nodes_meta[int(curr_node)]
                                    
                                    dist_m = haversine_np(u_info['lng'], u_info['lat'], v_info['lng'], v_info['lat'])
                                    speed_kmh = (dist_m / 1000) / (travel_time / 3600)
                                    
                                    # --- KHU VỰC SỬA LỖI ---
                                    # Ép kiểu thủ công về int/float python thuần
                                    edge_obj = {
                                        "f": int(last_node),    # int64 -> int
                                        "t": int(curr_node),    # int64 -> int
                                        "tm": round(float(travel_time), 1),
                                        "s": round(float(speed_kmh), 1)
                                    }
                                    
                                    if vid_str not in hourly_data[h]["vehicles"]: 
                                        hourly_data[h]["vehicles"][vid_str] = []
                                    hourly_data[h]["vehicles"][vid_str].append(edge_obj)
                                    
                                    # Key dictionary cũng phải là kiểu chuẩn (tuple of ints)
                                    ek = (int(last_node), int(curr_node))
                                    if ek not in agg_store[h]: agg_store[h][ek] = []
                                    agg_store[h][ek].append(speed_kmh)

                        if curr_node != last_node: last_node = curr_node
                        departure_time = curr_time

            # Save Chunks
            available_hours = []
            
            for h, data_h in hourly_data.items():
                aggs = []
                if h in agg_store:
                    for (u, v), speeds in agg_store[h].items():
                        avg_s = np.mean(speeds)
                        # u, v đã được ép kiểu int ở trên
                        u_info = nodes_meta[u]
                        v_info = nodes_meta[v]
                        dist_m = haversine_np(u_info['lng'], u_info['lat'], v_info['lng'], v_info['lat'])
                        avg_t = (dist_m / 1000) / (avg_s / 3600) if avg_s > 0 else 0
                        
                        aggs.append({
                            "f": int(u),
                            "t": int(v),
                            "s": round(float(avg_s), 1),
                            "tm": round(float(avg_t), 1),
                            "c": int(len(speeds))
                        })
                
                chunk_filename = f"{date_key}_{h}.json"
                with open(os.path.join(OUTPUT_DIR, chunk_filename), 'w') as f:
                    # Dùng NpEncoder để an toàn tuyệt đối
                    json.dump({"agg": aggs, "veh": data_h["vehicles"]}, f, cls=NpEncoder)
                
                available_hours.append(int(h))

            if available_hours:
                index_data[date_key] = sorted(available_hours)
                print(f"   -> OK: {date_key} ({len(available_hours)} khung giờ)")

        except Exception as e:
            print(f"⚠️ Lỗi file {filename}: {e}")
            import traceback
            traceback.print_exc()

    with open(os.path.join(OUTPUT_DIR, 'index.json'), 'w') as f:
        json.dump(index_data, f, cls=NpEncoder)
    
    print(f"✔ HOÀN TẤT! Dữ liệu: {OUTPUT_DIR}")