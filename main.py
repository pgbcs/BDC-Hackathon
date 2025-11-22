import genCluster as gc
import genMap as gm
import genPath as gp
import genFullMap as gf
import genCSV as gcsv
import pandas as pd
import app

def main():
    radius = 200
    root_folder = 'HCMC_bus_routes'  # Thư mục gốc chứa dữ liệu
    output_file = '../grouped_stops_nested.csv'
    # output_file = gc.group_stops_nested_structure(root_folder, radius_meters=radius)

    # gp.build_graph_with_unified_radius(output_file, '../raw_GPS/anonymized_raw_2025-04-01.csv', UNIFIED_RADIUS=radius)
    gf.create_sharded_traffic_map(output_file, '../raw_GPS', radius=radius)
    app.main()

if __name__ == "__main__":
    main()