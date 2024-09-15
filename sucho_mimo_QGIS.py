import os
import sys
import time

# Nastavení cesty k QGIS Python knihovnám
QGIS_PATH = r'D:\Program Files\QGIS 3.38.2\apps\qgis'
sys.path.append(QGIS_PATH + r'\python')
sys.path.append(QGIS_PATH + r'\python\plugins')

from qgis.core import (
    QgsApplication,
    QgsVectorLayer,
    QgsRasterLayer,
    QgsField,
    QgsVectorFileWriter,
    QgsCoordinateReferenceSystem
)
from qgis.analysis import QgsZonalStatistics
from PyQt5.QtCore import QVariant

def debug_print(message):
    print(f"[DEBUG] {time.strftime('%Y-%m-%d %H:%M:%S')} - {message}")

debug_print("Skript začíná")

# Inicializace QGIS aplikace
QgsApplication.setPrefixPath(QGIS_PATH, True)
qgs = QgsApplication([], False)
qgs.initQgis()
debug_print("QGIS aplikace inicializována")

# Cesty k souborům a složkám
shapefile_path = r"D:\OneDrive - CZU v Praze\SPS\Projekty\Agrometeorologie_cz\Sucho\Data\SHP\VYBRANY_POZEMEK_TIF2.cpg.shp"
raster_folder = r"D:\OneDrive - CZU v Praze\SPS\Projekty\Agrometeorologie_cz\Sucho\Data\Sucho-ze-serveru\2024_09_15"
output_folder = r"D:\OneDrive - CZU v Praze\SPS\Projekty\Agrometeorologie_cz\Sucho\Data\Save\Output"
result_shapefile_path = os.path.join(output_folder, "pozemky_aktualizovane.shp")
mysql_output_file = os.path.join(output_folder, "pozemky_data.sql")

debug_print(f"Shapefile cesta: {shapefile_path}")
debug_print(f"Rastrová složka: {raster_folder}")
debug_print(f"Výstupní složka: {output_folder}")
debug_print(f"Cesta k výslednému shapefile: {result_shapefile_path}")
debug_print(f"Cesta k MySQL výstupnímu souboru: {mysql_output_file}")

def shapefile_exists(output_folder):
    exists = os.path.exists(result_shapefile_path)
    debug_print(f"Kontrola existence výsledného shapefile: {'Existuje' if exists else 'Neexistuje'}")
    return exists

def export_to_mysql(shapefile_path, output_sql_file):
    debug_print(f"Začínám export do MySQL formátu: {output_sql_file}")
    
    layer = QgsVectorLayer(shapefile_path, "export_layer", "ogr")
    if not layer.isValid():
        debug_print("Chyba: Nelze načíst shapefile pro export do MySQL")
        return

    with open(output_sql_file, 'w', encoding='utf-8') as sql_file:
        sql_file.write("CREATE TABLE IF NOT EXISTS pozemky_data (\n")
        sql_file.write("  id INT AUTO_INCREMENT PRIMARY KEY,\n")
        
        fields = layer.fields()
        for field in fields:
            field_name = field.name()
            field_type = field.typeName()
            
            if field_type == 'Integer':
                sql_type = 'INT'
            elif field_type == 'Real':
                sql_type = 'DOUBLE'
            else:
                sql_type = 'VARCHAR(255)'
            
            sql_file.write(f"  {field_name} {sql_type},\n")
        
        sql_file.write(");\n\n")
        
        for feature in layer.getFeatures():
            attributes = feature.attributes()
            columns = ', '.join(field.name() for field in fields)
            values = ', '.join("'" + str(attr).replace("'", "''") + "'" for attr in attributes)
            sql_file.write(f"INSERT INTO pozemky_data ({columns}) VALUES ({values});\n")

    debug_print(f"Export do MySQL formátu dokončen: {output_sql_file}")

# Procházení všech rastrových souborů ve složce
raster_files = [f for f in sorted(os.listdir(raster_folder)) if f.endswith(".tif")]
debug_print(f"Nalezeno {len(raster_files)} rastrových souborů")

for index, raster_file in enumerate(raster_files, 1):
    debug_print(f"Zpracovávám rastr {index}/{len(raster_files)}: {raster_file}")
    raster_path = os.path.join(raster_folder, raster_file)
    raster_layer = QgsRasterLayer(raster_path, raster_file)

    if not raster_layer.isValid():
        debug_print(f"CHYBA: Rastrová vrstva {raster_path} je neplatná!")
        continue

    raster_base_name = os.path.splitext(raster_file)[0]
    raster_number = raster_base_name.split('_')[-1]
    debug_print(f"Extrahované číslo rastru: {raster_number}")

    if not shapefile_exists(output_folder):
        debug_print("Používám původní shapefile")
        polygon_layer = QgsVectorLayer(shapefile_path, "Polygon Layer", "ogr")
    else:
        debug_print("Používám existující výsledný shapefile")
        polygon_layer = QgsVectorLayer(result_shapefile_path, "Polygon Layer", "ogr")

    if not polygon_layer.isValid():
        debug_print(f"CHYBA: Shapefile vrstva {polygon_layer.sourceName()} je neplatná!")
        continue

    debug_print("Počítám zonální statistiky")
    mean_stats = QgsZonalStatistics(polygon_layer, raster_layer, '', 1, QgsZonalStatistics.Mean)
    mean_stats.calculateStatistics(None)

    majority_stats = QgsZonalStatistics(polygon_layer, raster_layer, '', 1, QgsZonalStatistics.Majority)
    majority_stats.calculateStatistics(None)

    debug_print("Přejmenovávám sloupce")
    polygon_layer.startEditing()
    fields = polygon_layer.fields()
    
    for field in fields:
        if "mean" in field.name().lower():
            idx = polygon_layer.fields().indexFromName(field.name())
            new_name = f'M_{raster_number}'
            polygon_layer.renameAttribute(idx, new_name)
            debug_print(f"Přejmenován sloupec: {field.name()} -> {new_name}")
        elif "majority" in field.name().lower():
            idx = polygon_layer.fields().indexFromName(field.name())
            new_name = f'N_{raster_number}'
            polygon_layer.renameAttribute(idx, new_name)
            debug_print(f"Přejmenován sloupec: {field.name()} -> {new_name}")
    
    polygon_layer.commitChanges()

    debug_print("Ukládám aktualizovaný shapefile")
    error = QgsVectorFileWriter.writeAsVectorFormat(polygon_layer, result_shapefile_path, "utf-8", polygon_layer.crs(), "ESRI Shapefile")

    if error == QgsVectorFileWriter.NoError:
        debug_print(f"Výsledný shapefile byl úspěšně uložen do: {result_shapefile_path}")
    else:
        debug_print(f"CHYBA: Při ukládání shapefile pro {raster_file} došlo k chybě")

debug_print("Exportuji data do MySQL formátu")
export_to_mysql(result_shapefile_path, mysql_output_file)

debug_print("Ukončuji QGIS aplikaci")
qgs.exitQgis()

debug_print("Skript dokončen")