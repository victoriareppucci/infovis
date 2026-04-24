import os
import pandas as pd

def main():
    input_path = 'data/visualizaciones/dataset_completo_viz.csv'
    
    if not os.path.exists(input_path):
        print(f"❌ No se encuentra {input_path}. Corré primero group_and_clean.py")
        return

    df = pd.read_csv(input_path)
    df['fecha_dt'] = pd.to_datetime(df['fecha_dt'])
    df = df.dropna(subset=['fecha_dt']).copy()
    df['cantidad'] = 1

    output_dir = 'data/visualizaciones'
    os.makedirs(output_dir, exist_ok=True)

    print("🚀 Generando datasets específicos para cada visualización...")

    # --- VIZ 1. DATAWRAPPER: Mapa de Picos ---
    df_dw = df.dropna(subset=['lat', 'lon']).copy()
    if not df_dw.empty:
        df_dw['lat_group'] = df_dw['lat'].round(3)
        df_dw['lon_group'] = df_dw['lon'].round(3)
        viz_1 = df_dw.groupby(['lat_group', 'lon_group', 'origen']).agg({
            'cantidad': 'sum', 'categoria': 'first'
        }).reset_index()
        viz_1.columns = ['lat', 'lon', 'origen', 'peso_pico', 'tipo']
        viz_1.to_csv(f'{output_dir}/viz_1_datawrapper_picos.csv', index=False)
        print("✅ 1. Datawrapper: viz_1_datawrapper_picos.csv")

    # --- VIZ 2. FLOURISH: Mapa Temporal ---
    df_fl = df.dropna(subset=['lat', 'lon']).copy()
    if not df_fl.empty:
        df_fl['fecha_iso'] = df_fl['fecha_dt'].dt.strftime('%Y-%m-%d')
        df_fl[['lat', 'lon', 'fecha_iso', 'origen', 'categoria']].to_csv(f'{output_dir}/viz_2_flourish_temporal.csv', index=False)
        print("✅ 2. Flourish: viz_2_flourish_temporal.csv")

    # --- VIZ 3. RAWGRAPHS: Barras Apiladas (Versión WIDE) ---
    df_bars = df.copy()
    # Creamos etiquetas de semanas claras (ej: "Sem 01")
    df_bars['semana_label'] = df_bars['fecha_dt'].dt.isocalendar().week.apply(lambda x: f"Sem {x:02d}")
    df_bars['dia_nro'] = df_bars['fecha_dt'].dt.dayofweek 
    
    dias_es = {0:'1. Lunes', 1:'2. Martes', 2:'3. Miércoles', 3:'4. Jueves', 
               4:'5. Viernes', 5:'6. Sábado', 6:'7. Domingo'}
    df_bars['dia_semana'] = df_bars['dia_nro'].map(dias_es)

    # Contamos fotos por día y semana
    df_counts = df_bars.groupby(['dia_semana', 'semana_label']).size().reset_index(name='cantidad')

    # TRANSFORMACIÓN CLAVE: Pasamos de formato largo a ancho
    viz_3_wide = df_counts.pivot(index='dia_semana', columns='semana_label', values='cantidad').fillna(0).reset_index()
    
    # Guardamos el nuevo CSV
    viz_3_wide.to_csv(f'{output_dir}/viz_3_rawgraphs_barras.csv', index=False)
    print("✅ 3. RAWGraphs: viz_3_rawgraphs_barras.csv (Versión Wide generada)")

    # --- VIZ 4. TABLEAU: Buenos Aires Rutina (Filtro por Día) ---
    df_ba = df[(df['lat'] < -33.2) & (df['lat'] > -41.0) & (df['lon'] < -55.0) & (df['lon'] > -63.5)].copy()
    if not df_ba.empty:
        dias_nombres = {
            'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles',
            'Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
        }
        df_ba['dia_nombre_es'] = df_ba['fecha_dt'].dt.day_name().map(dias_nombres)
        df_ba.to_csv(f'{output_dir}/viz_4_tableau_ba.csv', index=False)
        print("✅ 4. Tableau: viz_4_tableau_ba.csv")

    print("\n🎉 ¡Todos los archivos listos!")

if __name__ == "__main__":
    main()