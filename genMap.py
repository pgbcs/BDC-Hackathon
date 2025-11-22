import pandas as pd
import matplotlib.pyplot as plt
import json
import os

# Define file path
file_path = 'grouped_stops_nested.csv'

# Check if file exists
if not os.path.exists(file_path):
    print(f"File {file_path} not found.")
else:
    try:
        # Read the CSV
        df = pd.read_csv(file_path, skipinitialspace=True)
        
        # Aggregate data by cluster_label to create "Nodes"
        # We group by cluster_label and take the first centroid (since they are the same for the cluster)
        # We join unique Names, RouteIds, and Codes for the popup info
        nodes_df = df.groupby('cluster_label').agg({
            'centroid_lat': 'first',
            'centroid_lng': 'first',
            'Name': lambda x: ' | '.join(sorted(list(set(str(val).strip() for val in x if pd.notna(val))))),
            'RouteId': lambda x: ', '.join(sorted(list(set(str(val).strip() for val in x if pd.notna(val))))),
            'Code': lambda x: ', '.join(sorted(list(set(str(val).strip() for val in x if pd.notna(val)))))
        }).reset_index()
        
        print(f"Processed {len(df)} rows into {len(nodes_df)} unique nodes.")
        
        # --- 1. Create Static Map (Matplotlib) ---
        plt.figure(figsize=(12, 10))
        plt.scatter(nodes_df['centroid_lng'], nodes_df['centroid_lat'], 
                    c='red', s=40, alpha=0.7, edgecolors='black', linewidth=0.5)
        plt.title('Mạng lưới Trạm dừng Xe buýt (Bus Network Nodes)')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.grid(True, linestyle='--', alpha=0.5)
        
        # Save the plot
        plt.savefig('bus_nodes_static_map.png')
        print("Static map saved as 'bus_nodes_static_map.png'.")
        
        # --- 2. Create Interactive Map (Leaflet HTML) ---
        # Prepare data for JSON injection
        map_nodes = []
        for _, row in nodes_df.iterrows():
            map_nodes.append({
                'id': str(row['cluster_label']),
                'lat': row['centroid_lat'],
                'lng': row['centroid_lng'],
                'name': row['Name'],
                'routes': row['RouteId'],
                'codes': row['Code']
            })
            
        # Calculate map center
        center_lat = nodes_df['centroid_lat'].mean()
        center_lng = nodes_df['centroid_lng'].mean()
        
        # HTML Template with Leaflet
        html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Bus Network Map</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <style>
        body {{ margin: 0; padding: 0; }}
        #map {{ width: 100%; height: 100vh; }}
        .info-box {{
            padding: 6px 8px;
            font: 14px/16px Arial, Helvetica, sans-serif;
            background: white;
            background: rgba(255,255,255,0.8);
            box-shadow: 0 0 15px rgba(0,0,0,0.2);
            border-radius: 5px;
        }}
    </style>
</head>
<body>
    <div id="map"></div>
    <script>
        var map = L.map('map').setView([{center_lat}, {center_lng}], 13);

        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }}).addTo(map);

        var nodes = {json.dumps(map_nodes)};

        nodes.forEach(function(node) {{
            var popupContent = "<b>Node ID:</b> " + node.id + 
                               "<br><b>Tên trạm:</b> " + node.name + 
                               "<br><b>Các tuyến:</b> " + node.routes +
                               "<br><b>Mã trạm:</b> " + node.codes;
                               
            L.circleMarker([node.lat, node.lng], {{
                radius: 6,
                fillColor: "#ff7800",
                color: "#000",
                weight: 1,
                opacity: 1,
                fillOpacity: 0.8
            }}).addTo(map).bindPopup(popupContent);
        }});
    </script>
</body>
</html>
        """
        
        with open('bus_network_interactive.html', 'w', encoding='utf-8') as f:
            f.write(html_template)
        print("Interactive map saved as 'bus_network_interactive.html'.")

    except Exception as e:
        print(f"An error occurred: {e}")

