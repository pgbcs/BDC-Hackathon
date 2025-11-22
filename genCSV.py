import json
import os
import glob
import pandas as pd
import re

# --- CẤU HÌNH ---
INPUT_DIR = 'traffic_data_chunks'
OUT_NODES = 'traffic_nodes.csv'
OUT_EDGES = 'traffic_edges_summary.csv'
OUT_TRIPS = 'traffic_trips_detailed.csv'

def export_data():
    print("--- BẮT ĐẦU XUẤT DỮ LIỆU RA CSV ---")
    
    if not os.path.exists(INPUT_DIR):
        print(f"❌ Lỗi: Không tìm thấy thư mục '{INPUT_DIR}'")
        return

    # 1. XỬ LÝ NODES (TRẠM DỪNG)
    print("1. Đang xử lý danh sách Node...")
    nodes_file = os.path.join(INPUT_DIR, 'nodes.json')
    nodes_map = {}
    
    if os.path.exists(nodes_file):
        with open(nodes_file, 'r', encoding='utf-8') as f:
            nodes_data = json.load(f)
            
        node_rows = []
        for nid, info in nodes_data.items():
            nodes_map[str(nid)] = info['name'] # Map ID -> Name để dùng sau
            node_rows.append({
                'node_id': nid,
                'node_name': info['name'],
                'latitude': info['lat'],
                'longitude': info['lng']
            })
        
        pd.DataFrame(node_rows).to_csv(OUT_NODES, index=False, encoding='utf-8-sig')
        print(f"   -> Đã lưu {len(node_rows)} trạm vào '{OUT_NODES}'")
    else:
        print("❌ Không tìm thấy file nodes.json")
        return

    # 2. XỬ LÝ CHI TIẾT (EDGES & TRIPS)
    print("2. Đang quét dữ liệu phân mảnh...")
    
    all_files = glob.glob(os.path.join(INPUT_DIR, "*.json"))
    chunk_files = [f for f in all_files if "nodes.json" not in f and "index.json" not in f]
    
    agg_rows = []   # Danh sách tổng hợp
    trip_rows = []  # Danh sách chi tiết (có Vehicle ID)
    
    count = 0
    for file_path in chunk_files:
        filename = os.path.basename(file_path)
        # Parse Ngày và Giờ từ tên file: 2025-04-01_8.json
        match = re.search(r'(\d{4}-\d{2}-\d{2})_(\d+)', filename)
        if not match: continue
            
        date_str = match.group(1)
        hour_str = int(match.group(2))
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # A. Lấy dữ liệu TỔNG HỢP (Aggregated)
                if 'agg' in data:
                    for e in data['agg']:
                        agg_rows.append({
                            'date': date_str,
                            'hour': hour_str,
                            'from_id': e['f'],
                            'to_id': e['t'],
                            'from_name': nodes_map.get(str(e['f']), 'Unknown'),
                            'to_name': nodes_map.get(str(e['t']), 'Unknown'),
                            'avg_speed_kmh': e['s'],
                            'avg_time_sec': e['tm'],
                            'trip_count': e['c']
                        })

                # B. Lấy dữ liệu CHI TIẾT (Vehicles) -> CÓ VEHICLE ID
                if 'veh' in data:
                    # data['veh'] là dict: { "vehicle_id": [list of edges], ... }
                    for veh_id, edges in data['veh'].items():
                        for e in edges:
                            trip_rows.append({
                                'date': date_str,
                                'hour': hour_str,
                                'vehicle_id': veh_id,   # <--- ID XE Ở ĐÂY
                                'from_id': e['f'],
                                'to_id': e['t'],
                                'from_name': nodes_map.get(str(e['f']), 'Unknown'),
                                'to_name': nodes_map.get(str(e['t']), 'Unknown'),
                                'speed_kmh': e['s'],
                                'travel_time_sec': e['tm']
                            })
                            
        except Exception as e:
            print(f"⚠️ Lỗi file {filename}: {e}")
            
        count += 1
        if count % 100 == 0: print(f"   ... Đã đọc {count}/{len(chunk_files)} file", end='\r')

    # 3. LƯU FILE CSV
    print(f"\n3. Đang ghi file...")
    
    # Lưu file Tổng hợp
    if agg_rows:
        df_agg = pd.DataFrame(agg_rows)
        df_agg.sort_values(['date', 'hour', 'from_name']).to_csv(OUT_EDGES, index=False, encoding='utf-8-sig')
        print(f"✔ File Tổng hợp: {OUT_EDGES} ({len(df_agg)} dòng)")
        
    # Lưu file Chi tiết
    if trip_rows:
        df_trips = pd.DataFrame(trip_rows)
        df_trips.sort_values(['date', 'hour', 'vehicle_id']).to_csv(OUT_TRIPS, index=False, encoding='utf-8-sig')
        print(f"✔ File Chi tiết: {OUT_TRIPS} ({len(df_trips)} dòng)")