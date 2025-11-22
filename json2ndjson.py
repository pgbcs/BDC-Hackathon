import os, json

root="traffic_data_chunks"
out_files=[]

for fname in os.listdir(root):
    if fname.lower().endswith(".json"):
        src=os.path.join(root,fname)
        with open(src) as f:
            try:
                data=json.load(f)
            except Exception:
                continue
        lines=[]
        for e in data.get("agg",[]): lines.append(json.dumps(e))
        for vid,arr in data.get("veh",{}).items():
            for e in arr: lines.append(json.dumps({"vid":vid, **e}))
        dst=src.replace(".json",".ndjson")
        with open(dst,"w") as f: f.write("\n".join(lines))
        out_files.append(dst)

out_files
