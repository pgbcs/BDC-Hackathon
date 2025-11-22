import pandas as pd
import numpy as np
import networkx as nx
from scipy.spatial import cKDTree



def build_graph_with_unified_radius(nodes_file, gps_file, output_file='traffic_graph.gexf', UNIFIED_RADIUS=200):
    print(f"--- BẮT ĐẦU QUY TRÌNH VỚI BÁN KÍNH: {UNIFIED_RADIUS} MÉT ---")

    # 1. Load dữ liệu Node (Cluster)
    # Đây là kết quả của bước gom nhóm trước đó (cũng dùng UNIFIED_RADIUS)
    nodes_df = pd.read_csv(nodes_file)
    
    # Lấy tọa độ trung tâm của các Node
    unique_nodes = nodes_df.groupby('cluster_label').agg({
        'centroid_lat': 'first',
        'centroid_lng': 'first'
    }).reset_index()
    
    print(f"1. Đã load {len(unique_nodes)} node (Cluster).")

    # 2. Xây dựng KDTree (Spatial Indexing)
    # Chuyển đổi Lat/Lng sang hệ tọa độ Descartes (X, Y, Z) để tính khoảng cách mét chính xác
    R = 6371000 # Bán kính Trái Đất (m)
    phi = np.radians(unique_nodes['centroid_lat'])
    theta = np.radians(unique_nodes['centroid_lng'])
    
    unique_nodes['x'] = R * np.cos(phi) * np.cos(theta)
    unique_nodes['y'] = R * np.cos(phi) * np.sin(theta)
    unique_nodes['z'] = R * np.sin(phi)
    
    # Tạo cây tìm kiếm không gian
    tree = cKDTree(unique_nodes[['x', 'y', 'z']].values)
    
    # 3. Xử lý dữ liệu GPS
    print("2. Đang xử lý dữ liệu GPS...")
    gps_df = pd.read_csv(gps_file)
    gps_df['datetime'] = pd.to_datetime(gps_df['datetime'])
    
    # Chuyển đổi tọa độ GPS sang Descartes
    phi_gps = np.radians(gps_df['lat'])
    theta_gps = np.radians(gps_df['lng'])
    gps_x = R * np.cos(phi_gps) * np.cos(theta_gps)
    gps_y = R * np.cos(phi_gps) * np.sin(theta_gps)
    gps_z = R * np.sin(phi_gps)
    gps_coords = np.column_stack((gps_x, gps_y, gps_z))
    
    # --- QUAN TRỌNG: Map Matching dùng UNIFIED_RADIUS ---
    # distance_upper_bound đảm bảo chỉ lấy điểm nằm trong bán kính quy định
    dists, idxs = tree.query(gps_coords, k=1, distance_upper_bound=UNIFIED_RADIUS)
    
    # Gán Node ID cho các điểm GPS hợp lệ
    gps_df['node_id'] = -1 # -1 nghĩa là đang đi trên đường
    valid_mask = idxs < len(unique_nodes) # Chỉ lấy các index hợp lệ
    
    # Map từ index của KDTree sang Cluster Label thực tế
    idx_to_label = unique_nodes['cluster_label'].to_dict()
    gps_df.loc[valid_mask, 'node_id'] = gps_df.loc[valid_mask, 'nearest_node_idx'].map(idx_to_label) if 'nearest_node_idx' in gps_df else [idx_to_label[i] for i in idxs[valid_mask]]

    print(f"   -> Tỷ lệ map thành công: {valid_mask.sum()}/{len(gps_df)} điểm GPS.")

    # 4. Tính toán cạnh (Edges) và Thời gian di chuyển
    # Logic: Nếu xe chuyển từ Node A (node_id X) sang Node B (node_id Y) -> Tạo cạnh X->Y
    G = nx.DiGraph()
    edge_times = {} # Lưu danh sách thời gian di chuyển

    sorted_gps = gps_df.sort_values(['anonymized_vehicle', 'datetime'])
    
    for vid, group in sorted_gps.groupby('anonymized_vehicle'):
        last_node = -1
        departure_time = None
        
        for _, row in group.iterrows():
            curr_node = row['node_id']
            curr_time = row['datetime']
            
            if curr_node != -1: # Xe đang ở tại trạm
                # Nếu trước đó xe ở trạm khác -> Hoàn thành chuyến đi
                if last_node != -1 and last_node != curr_node:
                    travel_time = (curr_time - departure_time).total_seconds()
                    if 0 < travel_time < 7200: # Lọc nhiễu (chuyến đi < 2 tiếng)
                        edge = (last_node, curr_node)
                        if edge not in edge_times: edge_times[edge] = []
                        edge_times[edge].append(travel_time)
                
                # Cập nhật trạng thái
                if curr_node != last_node:
                    last_node = curr_node
                departure_time = curr_time # Cập nhật thời điểm cuối cùng thấy xe ở trạm này
    
    # 5. Tổng hợp đồ thị
    for (u, v), times in edge_times.items():
        G.add_edge(u, v, weight=np.mean(times), trips=len(times))
        
    print(f"3. Hoàn tất! Đồ thị có {G.number_of_nodes()} node và {G.number_of_edges()} cạnh.")
    nx.write_gexf(G, output_file)

# --- SỬ DỤNG ---
# build_graph_with_unified_radius('grouped_stops_nested.csv', 'gps_data.csv')