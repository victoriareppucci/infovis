import os
import pandas as pd
from PIL import Image
from pillow_heif import register_heif_opener
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata

register_heif_opener()

def get_decimal_coords(gps_info):
    def to_deg(value):
        try:
            if isinstance(value, (tuple, list)):
                return float(value[0]) + float(value[1])/60 + float(value[2])/3600
            return float(value)
        except: return None
    lat, lon = to_deg(gps_info.get(2)), to_deg(gps_info.get(4))
    if lat and gps_info.get(1) == 'S': lat = -lat
    if lon and gps_info.get(3) == 'W': lon = -lon
    return lat, lon

def limpiar_fecha_exif(fecha_str):
    if not fecha_str or pd.isna(fecha_str) or str(fecha_str).strip() in ['N/A', 'None', '']:
        return None
    s = str(fecha_str).strip()
    if len(s) >= 10 and s[4] == ':' and s[7] == ':':
        s = s[:10].replace(':', '-') + s[10:]
    return s

def main():
    path = 'data/fotos'
    if not os.path.exists(path):
        print(f"❌ ERROR: No se encuentra la carpeta '{path}'.")
        return

    capturas = {} 
    archivos = [f for f in os.listdir(path) if not f.startswith('.') and f != 'DS_Store']
    print(f"📂 Procesando {len(archivos)} archivos...")

    for file in archivos:
        nombre_base, ext_raw = os.path.splitext(file)
        ext = ext_raw.lower()
        f_path = os.path.join(path, file)

        if nombre_base not in capturas:
            capturas[nombre_base] = {
                'id_evento': nombre_base,
                'archivo_principal': file,
                'categoria': 'Otro',
                'origen': 'Externo (Descargado/Social)',
                'fecha_raw': None, 'lat': None, 'lon': None,
                'dispositivo': 'N/A', 'duracion_seg': None
            }

        curr = capturas[nombre_base]

        if ext in ['.heic', '.jpg', '.jpeg', '.png']:
            if curr['categoria'] != 'Video': curr['categoria'] = 'Imagen'
            try:
                with Image.open(f_path) as img:
                    exif = img.getexif()
                    if exif:
                        lat, lon = get_decimal_coords(exif.get_ifd(34853))
                        if lat: curr['lat'], curr['lon'] = lat, lon
                        if not curr['fecha_raw']: curr['fecha_raw'] = exif.get(306)
                        make = str(exif.get(271, '')).strip()
                        if make and any(x in make.lower() for x in ['apple', 'samsung', 'google']):
                            curr['origen'] = 'Interno (Captura Propia)'
                        curr['dispositivo'] = f"{make} {str(exif.get(272, ''))}".strip()
            except: pass
        elif ext in ['.mov', '.mp4']:
            curr['categoria'] = 'Video'
            try:
                parser = createParser(f_path)
                if parser:
                    with parser:
                        meta = extractMetadata(parser)
                        if meta:
                            d = meta.exportDictionary().get('Metadata', {})
                            if not curr['fecha_raw']: curr['fecha_raw'] = d.get('Creation date')
                            curr['duracion_seg'] = d.get('Duration')
            except: pass

    # --- PROCESAMIENTO PANDAS ---
    df = pd.DataFrame(list(capturas.values()))
    df = df[df['categoria'] != 'Otro']
    df['fecha_dt'] = pd.to_datetime(df['fecha_raw'].apply(limpiar_fecha_exif), errors='coerce')
    df['hora'] = df['fecha_dt'].dt.hour
    df['dia_semana'] = df['fecha_dt'].dt.day_name()
    df['cantidad'] = 1 # Para picos en RAWGraphs
    
    # 1. Exportar Dataset Completo (para RAWGraphs y Flourish)
    os.makedirs('data/visualizaciones', exist_ok=True)
    df.sort_values('fecha_dt').to_csv('data/visualizaciones/dataset_completo_viz.csv', index=False)
    
    print(f"✅ ¡Proceso terminado! Archivo listo en data/visualizaciones/ con {len(df)} registros.")

if __name__ == "__main__":
    main()