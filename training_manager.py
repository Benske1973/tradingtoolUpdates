# # ===============================================================
# # Training Management System v10 - PyQt6 (FIXED VERSION)
# # Dashboard + Medewerkerbeheer + Planner/To-do (automatisch + zwevend)
# # ===============================================================
#
# (originele header en imports bewaard)
#
import os
import math
import webbrowser
import re

from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from PyQt6.QtGui import QDesktopServices, QColor
from PyQt6.QtCore import QUrl
from sqlalchemy import create_engine, text

import pandas as pd

# in trainingtest.py (bovenaan bij imports)


from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QComboBox, QLineEdit, QCheckBox,
    QScrollArea, QMessageBox, QGroupBox, QListWidget, QListWidgetItem,
    QDialog, QDialogButtonBox, QToolButton, QButtonGroup,
    QStackedWidget, QFrame, QDateEdit, QTextEdit, QInputDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView, QSizePolicy,
    )

from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QPointF, QRectF, QDate, QUrl
from PyQt6.QtGui import (
    QIcon, QPainter, QPixmap, QColor,
    QPen, QPainterPath, QRadialGradient,
    QDesktopServices, QBrush, QLinearGradient, QFontMetrics,
)
from dataclasses import dataclass

# ===============================================================
# PADEN / SETTINGS
# ===============================================================

from xaurum.theme import APP_STYLE, load_logo_icon
from xaurum.config import *
from xaurum.utils import *

class SQLServerTrainingManager:
    """
    SQL Server manager voor alle training-gerelateerde tabellen.
    (Bijgewerkt: defensieve leestaken en veilige save/merge voor TodoPlanner)
    """

    def __init__(self, server: str, database: str):
        self.server = server
        self.database = database
        self.engine = None
        self._init_engine()

    def _init_engine(self):
        """Initialiseer SQLAlchemy engine."""
        try:
            from sqlalchemy import create_engine, text
            import urllib.parse

            # 🔧 FIX: Gebruik de moderne driver (ODBC Driver 17) i.p.v. de oude legacy driver
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=Yes;"
            )

            # URL encode de connection string
            params = urllib.parse.quote_plus(conn_str)

            # Create engine MET correcte URL
            self.engine = create_engine(
                f"mssql+pyodbc:///?odbc_connect={params}",
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600
            )

            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()

            print(f"✅ SQL Training Connectie: {self.server}/{self.database}")
            print(f"✅ SQL Training Manager gereed")

        except Exception as e:
            print(f"❌ SQL Training Connectie fout: {e}")
            import traceback
            traceback.print_exc()
            self.engine = None
    
    def _connect(self):
        """
        Return een database connectie voor gebruik in 'with' statement.

        Returns:
            sqlalchemy.engine.Connection of None
        """
        if self.engine is None:
            # Probeer opnieuw te initialiseren
            self._init_engine()

            if self.engine is None:
                print("âŒ _connect(): Engine niet beschikbaar")
                return None

        # âœ… Return de connectie
        return self.engine.connect()

    def is_available(self) -> bool:
        """Check of connectie beschikbaar is."""
        if self.engine is None:
            return False
        try:
            from sqlalchemy import text
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except:
            return False

    # ==========================
    # Master / Config / Catalog
    # ==========================
    # (ongewijzigde functies voor master/cfg/catalog blijven hier â€” beknopt weergegeven)
    def get_master_certificaten(self) -> pd.DataFrame:
        if not self.engine:
            return pd.DataFrame()
        try:
            query = """
                SELECT 
                    CertificaatID,
                    CertName,
                    CertName_norm,
                    Active,
                    StrategischBelangrijk,
                    Categorie,
                    Geldigheid_maanden,
                    Opmerking
                FROM dbo.TM_MasterCertificaten
                WHERE Active = 1
                ORDER BY CertName
            """
            df = pd.read_sql(query, self.engine)
            print(f"âœ… SQL: {len(df)} master certificaten")
            return df
        except Exception as e:
            print(f"âŒ SQL get_master_certificaten fout: {e}")
            return pd.DataFrame()

    def get_master_competenties(self) -> pd.DataFrame:
        if not self.engine:
            return pd.DataFrame()
        try:
            query = """
                SELECT 
                    Competence,
                    Competence_norm,
                    Active,
                    StrategischBelangrijk,
                    Categorie,
                    Opmerking
                FROM dbo.TM_MasterCompetenties
                WHERE Active = 1
                ORDER BY Competence
            """
            df = pd.read_sql(query, self.engine)
            print(f"âœ… SQL: {len(df)} master competenties")
            return df
        except Exception as e:
            print(f"âŒ SQL get_master_competenties fout: {e}")
            return pd.DataFrame()

    def get_training_catalogus(self) -> pd.DataFrame:
        if not self.engine:
            return pd.DataFrame()
        try:
            query = """
                SELECT 
                    TrainingID,
                    title,
                    url,
                    code,
                    raw_text,
                    CertName_norm
                FROM dbo.TM_TrainingCatalogus
                ORDER BY title
            """
            df = pd.read_sql(query, self.engine)
            print(f"âœ… SQL: {len(df)} trainingen in catalogus")
            return df
        except Exception as e:
            print(f"âŒ SQL get_training_catalogus fout: {e}")
            return pd.DataFrame()

    # ==========================
    # Medewerker Config
    # ==========================
    def get_medewerker_certificaat_config(self, staff_gid: str = None) -> pd.DataFrame:
        if not self.engine:
            return pd.DataFrame()
        try:
            if staff_gid:
                query = """
                    SELECT 
                        ConfigID,
                        staffGID,
                        staffSAPNR,
                        FullName,
                        CertName,
                        CertName_norm,
                        Nodig,
                        Strategisch,
                        Interval_maanden,
                        Opmerking,
                        LaatsteWijziging,
                        GewijzigdDoor
                    FROM dbo.TM_MedewerkerCertificaatConfig
                    WHERE staffGID = ?
                    ORDER BY CertName
                """
                df = pd.read_sql(query, self.engine, params=(staff_gid,))
            else:
                query = """
                    SELECT 
                        ConfigID,
                        staffGID,
                        staffSAPNR,
                        FullName,
                        CertName,
                        CertName_norm,
                        Nodig,
                        Strategisch,
                        Interval_maanden,
                        Opmerking,
                        LaatsteWijziging,
                        GewijzigdDoor
                    FROM dbo.TM_MedewerkerCertificaatConfig
                    ORDER BY staffGID, CertName
                """
                df = pd.read_sql(query, self.engine)
            print(f"âœ… SQL: {len(df)} certificaat config rijen")
            return df
        except Exception as e:
            print(f"âŒ SQL get_medewerker_certificaat_config fout: {e}")
            return pd.DataFrame()

    def get_medewerker_competentie_config(self, staff_gid: str = None) -> pd.DataFrame:
        if not self.engine:
            return pd.DataFrame()
        try:
            from sqlalchemy import text
            if staff_gid:
                query = text("""
                    SELECT 
                        ConfigID,
                        staffGID,
                        staffSAPNR,
                        FullName,
                        Competence,
                        Competence_norm,
                        Nodig,
                        Interval_maanden,
                        Opmerking,
                        LaatsteWijziging,
                        GewijzigdDoor
                    FROM dbo.TM_MedewerkerCompetentieConfig
                    WHERE staffGID = :gid
                    ORDER BY Competence
                """)
                with self.engine.connect() as conn:
                    result = conn.execute(query, {'gid': staff_gid})
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    print(f"âœ… get_medewerker_competentie_config({staff_gid}): {len(df)} rijen")
                    return df
            else:
                query = text("""
                    SELECT 
                        ConfigID,
                        staffGID,
                        staffSAPNR,
                        FullName,
                        Competence,
                        Competence_norm,
                        Nodig,
                        Interval_maanden,
                        Opmerking,
                        LaatsteWijziging,
                        GewijzigdDoor
                    FROM dbo.TM_MedewerkerCompetentieConfig
                    ORDER BY FullName, Competence
                """)
                with self.engine.connect() as conn:
                    result = conn.execute(query)
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    print(f"âœ… get_medewerker_competentie_config (alle): {len(df)} rijen")
                    return df
        except Exception as e:
            print(f"âŒ get_medewerker_competentie_config fout: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def save_medewerker_certificaat_config(self, df: pd.DataFrame) -> bool:
        """
        Slaat certificaat configuratie op naar SQL Server.

        - Robuuste/veilige versie: per-rij transacties zodat 1 fout niet alles blokkeert.
        - Behandelt ontbrekende kolommen, NaNs en normalisatie.
        - Gebruikt connection.begin() per rij zodat commit/rollback expliciet is.
        """
        from sqlalchemy import text
        import pandas as pd
        from datetime import datetime
        import traceback
        import os

        if df is None or df.empty:
            print("âš ï¸ save_medewerker_certificaat_config: Geen data om op te slaan")
            return True

        print(f"\nðŸ’¾ save_medewerker_certificaat_config: {len(df)} rijen opslaan...")

        # -------------- helper functies --------------
        def clean_string(val, default=""):
            if pd.isna(val) or val is None:
                return default
            return str(val).strip()

        def clean_bool(val):
            if pd.isna(val) or val is None:
                return False
            if isinstance(val, bool):
                return val
            if isinstance(val, (int, float)):
                return bool(val)
            if isinstance(val, str):
                return val.lower() in ('true', '1', 'yes', 'ja', 'aan', 'on')
            return bool(val)

        def clean_int(val, default=0):
            if pd.isna(val) or val is None:
                return default
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return default

        def format_medewerker_naam_local(full_name: str) -> str:
            """
            Converteert FullName naar MedewerkerNaam formaat.
            "ACHTERNAAM, Voornaam" â†’ "Achternaam Voornaam"
            """
            if not full_name or not isinstance(full_name, str):
                return ""
            full_name = full_name.strip()
            if not full_name:
                return ""
            if ',' in full_name:
                parts = full_name.split(',', 1)
                if len(parts) == 2:
                    achternaam = parts[0].strip().title()
                    voornaam = parts[1].strip().title()
                    return f"{achternaam} {voornaam}"
            parts = full_name.split()
            if len(parts) >= 2 and parts[0].isupper():
                achternaam = parts[0].title()
                voornaam = ' '.join(parts[1:]).title()
                return f"{achternaam} {voornaam}"
            return full_name.title()

        # fallback USERNAME
        try:
            USER = USERNAME  # probeer globale USERNAME (zoals in jouw repo)
        except Exception:
            try:
                USER = os.getlogin()
            except Exception:
                USER = "system"

        success_count = 0
        error_count = 0

        # zorg dat engine bestaat
        if getattr(self, "engine", None) is None:
            print("âŒ save_medewerker_certificaat_config: Geen SQL engine beschikbaar")
            return False

        # We gebruiken per-rij transacties zodat 1 fout niet de hele batch faalt.
        # Gebruik Connection.begin() zodat we transacties beheersen.
        try:
            with self.engine.connect() as conn:
                for idx, row in df.iterrows():
                    try:
                        staff_gid = clean_string(row.get('staffGID'))
                        staff_sapnr = clean_string(row.get('staffSAPNR'))
                        full_name = clean_string(row.get('FullName'))

                        medewerker_naam = clean_string(row.get('MedewerkerNaam'))
                        if not medewerker_naam and full_name:
                            medewerker_naam = format_medewerker_naam_local(full_name)

                        if not medewerker_naam:
                            if full_name:
                                medewerker_naam = full_name.title()
                            else:
                                medewerker_naam = f"Medewerker {staff_gid}"

                        cert_name = clean_string(row.get('CertName'))
                        cert_name_norm = clean_string(row.get('CertName_norm'))
                        if not cert_name_norm and cert_name:
                            # gebruik interne normalizer indien aanwezig
                            if hasattr(self, "_normalize_certname"):
                                cert_name_norm = self._normalize_certname(cert_name)
                            else:
                                cert_name_norm = re.sub(r'[^a-z0-9]', '', cert_name.lower())

                        nodig = clean_bool(row.get('Nodig'))
                        strategisch = clean_bool(row.get('Strategisch'))
                        interval_maanden = clean_int(row.get('Interval_maanden'), 0)
                        opmerking = clean_string(row.get('Opmerking'))

                        laatste_wijziging = row.get('LaatsteWijziging')
                        if pd.isna(laatste_wijziging) or laatste_wijziging is None:
                            laatste_wijziging = datetime.now()

                        gewijzigd_door = clean_string(row.get('GewijzigdDoor'))
                        if not gewijzigd_door:
                            gewijzigd_door = USER

                        # validatie
                        if not staff_gid:
                            print(f"   âš ï¸ Rij {idx}: Skipped - staffGID ontbreekt")
                            error_count += 1
                            continue
                        if not cert_name:
                            print(f"   âš ï¸ Rij {idx}: Skipped - CertName ontbreekt")
                            error_count += 1
                            continue

                        # start transaction per rij
                        trans = conn.begin()
                        try:
                            check_sql = text("""
                                SELECT ConfigID FROM dbo.TM_MedewerkerCertificaatConfig
                                WHERE staffGID = :gid AND CertName = :cert
                            """)
                            result = conn.execute(check_sql, {"gid": staff_gid, "cert": cert_name})
                            existing = result.fetchone()

                            if existing:
                                update_sql = text("""
                                    UPDATE dbo.TM_MedewerkerCertificaatConfig
                                    SET staffSAPNR = :sapnr,
                                        FullName = :fullname,
                                        MedewerkerNaam = :medewerkernaam,
                                        CertName_norm = :certnorm,
                                        Nodig = :nodig,
                                        Strategisch = :strategisch,
                                        Interval_maanden = :interval,
                                        Opmerking = :opmerking,
                                        LaatsteWijziging = :wijziging,
                                        GewijzigdDoor = :door
                                    WHERE staffGID = :gid AND CertName = :cert
                                """)
                                conn.execute(update_sql, {
                                    'sapnr': staff_sapnr or None,
                                    'fullname': full_name or None,
                                    'medewerkernaam': medewerker_naam or None,
                                    'certnorm': cert_name_norm or None,
                                    'nodig': 1 if nodig else 0,
                                    'strategisch': 1 if strategisch else 0,
                                    'interval': interval_maanden,
                                    'opmerking': opmerking or None,
                                    'wijziging': laatste_wijziging,
                                    'door': gewijzigd_door,
                                    'gid': staff_gid,
                                    'cert': cert_name
                                })
                                print(f"   âœï¸ UPDATE: {cert_name} voor {staff_gid}")
                                success_count += 1
                            else:
                                insert_sql = text("""
                                    INSERT INTO dbo.TM_MedewerkerCertificaatConfig (
                                        staffGID, staffSAPNR, FullName, MedewerkerNaam,
                                        CertName, CertName_norm,
                                        Nodig, Strategisch, Interval_maanden, Opmerking,
                                        DatumToegevoegd, LaatsteWijziging, GewijzigdDoor
                                    ) VALUES (
                                        :gid, :sapnr, :fullname, :medewerkernaam,
                                        :cert, :certnorm,
                                        :nodig, :strategisch, :interval, :opmerking,
                                        GETDATE(), :wijziging, :door
                                    )
                                """)
                                conn.execute(insert_sql, {
                                    'gid': staff_gid,
                                    'sapnr': staff_sapnr or None,
                                    'fullname': full_name or None,
                                    'medewerkernaam': medewerker_naam or None,
                                    'cert': cert_name,
                                    'certnorm': cert_name_norm or None,
                                    'nodig': 1 if nodig else 0,
                                    'strategisch': 1 if strategisch else 0,
                                    'interval': interval_maanden,
                                    'opmerking': opmerking or None,
                                    'wijziging': laatste_wijziging,
                                    'door': gewijzigd_door
                                })
                                print(f"   âž• INSERT: {cert_name} voor {staff_gid}")
                                success_count += 1

                            trans.commit()
                        except Exception as row_exc:
                            try:
                                trans.rollback()
                            except Exception:
                                pass
                            print(f"   âŒ Rij {idx} fout (DB): {row_exc}")
                            traceback.print_exc()
                            error_count += 1
                            # ga door naar volgende rij
                    except Exception as inner:
                        print(f"   âŒ Rij {idx} fout (verwerking): {inner}")
                        traceback.print_exc()
                        error_count += 1
                        continue

            print(f"\nâœ… save_medewerker_certificaat_config: {success_count} OK, {error_count} fouten")
            return error_count == 0

        except Exception as e:
            print(f"âŒ save_medewerker_certificaat_config FATALE FOUT: {e}")
            traceback.print_exc()
            return False
    

    # def save_medewerker_competentie_config(self, df: pd.DataFrame) -> bool:
        # """
        # Slaat competenties op in TM_MedewerkerCompetentieConfig.
        # Geoptimaliseerd met MERGE statement voor snelheid en PK-veiligheid.
        # """
        # from sqlalchemy import text
        # import pandas as pd
        # from datetime import datetime
        # import traceback
        # import re
        # import os

        # if df is None or df.empty:
            # return True

        # print(f"\n💾 save_medewerker_competentie_config: {len(df)} rijen verwerken...")

        # # Helper functies
        # def clean_string(val): return str(val).strip() if pd.notna(val) else ""
        # def clean_bool(val): 
            # if pd.isna(val): return False
            # return val if isinstance(val, bool) else str(val).lower() in ('true', '1', 'yes')
        # def clean_int(val): 
            # try: return int(float(val)) 
            # except: return 0

        # # Bepaal USERNAME
        # try: 
            # from xaurum.config import USERNAME
            # USER = USERNAME
        # except: 
            # try: USER = os.getlogin()
            # except: USER = "system"

        # if getattr(self, "engine", None) is None:
            # return False

        # # SQL Query met MERGE (Upsert)
        # merge_sql = text("""
            # DECLARE @officialName NVARCHAR(255) = (SELECT TOP 1 (staffLASTNAME + ', ' + staffFIRSTNAME) 
                                                  # FROM dbo.tblSTAFF WHERE staffGID = :gid);
            
            # MERGE INTO dbo.TM_MedewerkerCompetentieConfig AS target
            # USING (SELECT :gid AS staffGID, :comp AS Competence) AS source
            # ON (target.staffGID = source.staffGID AND target.Competence = source.Competence)
            
            # WHEN MATCHED THEN
                # UPDATE SET 
                    # staffSAPNR = :sapnr,
                    # MedewerkerNaam = ISNULL(@officialName, :mnaam),
                    # FullName = :fullname,
                    # Competence_norm = :norm,
                    # Nodig = :nodig,
                    # Interval_maanden = :interval,
                    # Opmerking = :opmerking,
                    # LaatsteWijziging = GETDATE(),
                    # GewijzigdDoor = :door
            
            # WHEN NOT MATCHED THEN
                # INSERT (staffGID, staffSAPNR, FullName, MedewerkerNaam, Competence, Competence_norm, Nodig, Interval_maanden, Opmerking, LaatsteWijziging, GewijzigdDoor)
                # VALUES (:gid, :sapnr, :fullname, ISNULL(@officialName, :mnaam), :comp, :norm, :nodig, :interval, :opmerking, GETDATE(), :door);
        # """)

        # try:
            # # Gebruik .begin() voor een automatische transactie over de hele loop
            # with self.engine.begin() as conn:
                # for idx, row in df.iterrows():
                    # staff_gid = clean_string(row.get('staffGID'))
                    # comp_name = clean_string(row.get('Competence'))
                    
                    # if not staff_gid or not comp_name:
                        # continue

                    # comp_norm = clean_string(row.get('Competence_norm'))
                    # if not comp_norm:
                        # comp_norm = re.sub(r'[^a-z0-9]', '', comp_name.lower())

                    # params = {
                        # 'gid': staff_gid,
                        # 'sapnr': clean_string(row.get('staffSAPNR')),
                        # 'fullname': clean_string(row.get('FullName')),
                        # 'mnaam': clean_string(row.get('MedewerkerNaam')),
                        # 'comp': comp_name,
                        # 'norm': comp_norm,
                        # 'nodig': 1 if clean_bool(row.get('Nodig')) else 0,
                        # 'interval': clean_int(row.get('Interval_maanden')),
                        # 'opmerking': clean_string(row.get('Opmerking')),
                        # 'door': USER
                    # }
                    
                    # conn.execute(merge_sql, params)

            # print(f"✅ Competenties succesvol gesynchroniseerd met SQL database.")
            # return True

        # except Exception as e:
            # print(f"🔥 Fatale fout bij opslaan competenties: {e}")
            # traceback.print_exc()
            # return False
    # def save_medewerker_competentie_config(self, df:  pd.DataFrame) -> bool:
        # """
        # Slaat competenties op in TM_MedewerkerCompetentieConfig. 
        # """
        # from sqlalchemy import text
        # import pandas as pd
        # import traceback
        # import re
        # import os

        # if df is None or df.empty:
            # print("   ⚠️ save_medewerker_competentie_config:  Geen data om op te slaan")
            # return True

        # print(f"\n💾 save_medewerker_competentie_config: {len(df)} rijen verwerken...")
        # print(f"   📋 DEBUG - Kolommen in DataFrame: {list(df.columns)}")
        
        # # ✅ DEBUG: Toon eerste rij
        # if len(df) > 0:
            # print(f"   📋 DEBUG - Eerste rij data:")
            # for col in df.columns:
                # print(f"      • {col}:  '{df. iloc[0]. get(col, 'N/A')}'")

        # # Helper functies
        # def clean_string(val): 
            # if pd.isna(val) or val is None: 
                # return ""
            # return str(val).strip()
        
        # def clean_bool(val): 
            # if pd.isna(val): 
                # return False
            # if isinstance(val, bool):
                # return val
            # return str(val).lower() in ('true', '1', 'yes', 'ja', 'aan', 'on')
        
        # def clean_int(val): 
            # try:  
                # return int(float(val)) 
            # except:  
                # return 0

        # # Bepaal USERNAME
        # try:  
            # from xaurum.config import USERNAME
            # USER = USERNAME
        # except:  
            # try: 
                # USER = os.getlogin()
            # except: 
                # USER = "system"

        # if getattr(self, "engine", None) is None:
            # print("   ❌ Geen SQL engine beschikbaar")
            # return False

        # success_count = 0
        # error_count = 0

        # # Check query
        # check_sql = text("""
            # SELECT ConfigID FROM dbo.TM_MedewerkerCompetentieConfig
            # WHERE staffGID = :gid AND Competence = :comp
        # """)

        # # Insert query
        # insert_sql = text("""
            # INSERT INTO dbo. TM_MedewerkerCompetentieConfig (
                # staffGID, staffSAPNR, FullName, MedewerkerNaam, 
                # Competence, Competence_norm, 
                # Nodig, Interval_maanden, Opmerking, 
                # LaatsteWijziging, GewijzigdDoor
            # )
            # VALUES (
                # :gid, :sapnr, :fullname, : mnaam, 
                # :comp, :norm, 
                # :nodig, :interval, :opmerking, 
                # GETDATE(), :door
            # )
        # """)

        # # Update query
        # update_sql = text("""
            # UPDATE dbo. TM_MedewerkerCompetentieConfig
            # SET staffSAPNR = :sapnr,
                # FullName = :fullname,
                # MedewerkerNaam = :mnaam,
                # Competence_norm = :norm,
                # Nodig = :nodig,
                # Interval_maanden = : interval,
                # Opmerking = :opmerking,
                # LaatsteWijziging = GETDATE(),
                # GewijzigdDoor = :door
            # WHERE staffGID = :gid AND Competence = :comp
        # """)

        # try:
            # with self.engine.connect() as conn:
                # for idx, row in df.iterrows():
                    # staff_gid = clean_string(row.get('staffGID'))
                    
                    # # ✅ FIX: Probeer meerdere kolomnamen
                    # comp_name = clean_string(row.get('Competence'))
                    # if not comp_name:
                        # comp_name = clean_string(row.get('CertName'))
                    # if not comp_name:
                        # comp_name = clean_string(row.get('competence'))
                    
                    # print(f"   🔍 Rij {idx}:  staffGID='{staff_gid}', Competence='{comp_name}'")
                    
                    # if not staff_gid or not comp_name:
                        # print(f"   ⚠️ Rij {idx}:  SKIPPED - staffGID of Competence leeg")
                        # error_count += 1
                        # continue

                    # # Normalisatie
                    # comp_norm = clean_string(row.get('Competence_norm'))
                    # if not comp_norm: 
                        # comp_norm = clean_string(row.get('CertName_norm'))
                    # if not comp_norm:
                        # comp_norm = re.sub(r'[^a-z0-9]', '', comp_name. lower())

                    # # Start transactie
                    # trans = conn.begin()
                    # try:
                        # # Check of record bestaat
                        # check_result = conn.execute(check_sql, {"gid": staff_gid, "comp": comp_name})
                        # existing = check_result.fetchone()
                        
                        # params = {
                            # 'gid': staff_gid,
                            # 'sapnr':  clean_string(row.get('staffSAPNR')),
                            # 'fullname': clean_string(row.get('FullName')),
                            # 'mnaam': clean_string(row.get('MedewerkerNaam')),
                            # 'comp': comp_name,
                            # 'norm': comp_norm,
                            # 'nodig': 1 if clean_bool(row. get('Nodig')) else 0,
                            # 'interval':  clean_int(row.get('Interval_maanden')),
                            # 'opmerking': clean_string(row.get('Opmerking')),
                            # 'door': USER
                        # }
                        
                        # if existing:
                            # conn.execute(update_sql, params)
                            # print(f"   ✏️ UPDATE: {comp_name} voor {staff_gid}")
                        # else:
                            # conn. execute(insert_sql, params)
                            # print(f"   ➕ INSERT: {comp_name} voor {staff_gid}")
                        
                        # trans.commit()
                        # success_count += 1
                        
                    # except Exception as row_exc:
                        # try:
                            # trans.rollback()
                        # except:
                            # pass
                        # print(f"   ❌ Rij {idx} SQL fout: {row_exc}")
                        # traceback.print_exc()
                        # error_count += 1

            # print(f"\n✅ save_medewerker_competentie_config: {success_count} OK, {error_count} fouten")
            # return error_count == 0

        # except Exception as e:
            # print(f"🔥 Fatale fout bij opslaan competenties: {e}")
            # traceback.print_exc()
            # return False
    def save_medewerker_competentie_config(self, df: pd.DataFrame) -> bool:
        """
        Slaat competenties op in TM_MedewerkerCompetentieConfig. 
        """
        from sqlalchemy import text
        import pandas as pd
        import traceback
        import re
        import os

        if df is None or df.empty:
            print("   ⚠️ save_medewerker_competentie_config: Geen data om op te slaan")
            return True

        print(f"\n💾 save_medewerker_competentie_config:{len(df)} rijen verwerken...")
        print(f"   📋 DEBUG - Kolommen in DataFrame: {list(df.columns)}")

        # Helper functies
        def clean_string(val):
            if pd.isna(val) or val is None:
                return ""
            s = str(val).strip()
            return "" if s.lower() == "nan" else s

        def clean_bool(val):
            if pd.isna(val):
                return False
            if isinstance(val, bool):
                return val
            return str(val).lower() in ('true', '1', 'yes', 'ja', 'aan', 'on')

        def clean_int(val):
            try:
                return int(float(val))
            except:
                return 0

        # Bepaal USERNAME
        try:
            from xaurum.config import USERNAME
            USER = USERNAME
        except:
            try:
                USER = os.getlogin()
            except:
                USER = "system"

        if getattr(self, "engine", None) is None:
            print("   ❌ Geen SQL engine beschikbaar")
            return False

        success_count = 0
        error_count = 0

        check_sql = text("""
            SELECT ConfigID FROM dbo.TM_MedewerkerCompetentieConfig
            WHERE staffGID = :gid AND Competence = :comp
        """)

        insert_sql = text("""
            INSERT INTO dbo.TM_MedewerkerCompetentieConfig (
                staffGID, staffSAPNR, FullName, MedewerkerNaam,
                Competence, Competence_norm,
                Nodig, Interval_maanden, Opmerking,
                LaatsteWijziging, GewijzigdDoor
            )
            VALUES (
                :gid, :sapnr, :fullname, :mnaam,
                :comp, :norm,
                :nodig, :interv, :opmerking,
                GETDATE(), :door
            )
        """)

        update_sql = text("""
            UPDATE dbo.TM_MedewerkerCompetentieConfig
            SET staffSAPNR = :sapnr,
                FullName = :fullname,
                MedewerkerNaam = :mnaam,
                Competence_norm = :norm,
                Nodig = :nodig,
                Interval_maanden = :interv,
                Opmerking = :opmerking,
                LaatsteWijziging = GETDATE(),
                GewijzigdDoor = :door
            WHERE staffGID = :gid AND Competence = :comp
        """)

        try:
            with self.engine.connect() as conn:
                for idx, row in df.iterrows():
                    staff_gid = clean_string(row.get('staffGID'))

                    # Probeer meerdere kolomnamen
                    comp_name = clean_string(row.get('Competence'))
                    if not comp_name:
                        comp_name = clean_string(row.get('CertName'))
                    if not comp_name:
                        comp_name = clean_string(row.get('competence'))

                    if not staff_gid or not comp_name:
                        print(f"   ⚠️ Rij {idx}: SKIPPED - staffGID of Competence leeg")
                        error_count += 1
                        continue

                    # Normalisatie
                    comp_norm = clean_string(row.get('Competence_norm'))
                    if not comp_norm:
                        comp_norm = clean_string(row.get('CertName_norm'))
                    if not comp_norm:
                        comp_norm = re.sub(r'[^a-z0-9]', '', comp_name.lower())

                    # Start transactie
                    trans = conn.begin()
                    try:
                        # Check of record bestaat
                        check_result = conn.execute(check_sql, {"gid":staff_gid, "comp":comp_name})
                        existing = check_result.fetchone()

                        params = {
                            'gid':staff_gid,
                            'sapnr':clean_string(row.get('staffSAPNR')),
                            'fullname':clean_string(row.get('FullName')),
                            'mnaam': clean_string(row.get('MedewerkerNaam')),
                            'comp':comp_name,
                            'norm':comp_norm,
                            'nodig':1 if clean_bool(row.get('Nodig')) else 0,
                            'interv':clean_int(row.get('Interval_maanden')),
                            'opmerking':clean_string(row.get('Opmerking')),
                            'door':USER
                        }

                        if existing:
                            conn.execute(update_sql, params)
                            print(f"   ✏️ UPDATE:{comp_name} voor {staff_gid}")
                        else:
                            conn.execute(insert_sql, params)
                            print(f"   ➕ INSERT:{comp_name} voor {staff_gid}")

                        trans.commit()
                        success_count += 1

                    except Exception as row_exc:
                        try:
                            trans.rollback()
                        except:
                            pass
                        print(f"   ❌ Rij {idx} SQL fout:{row_exc}")
                        traceback.print_exc()
                        error_count += 1

            print(f"\n✅ save_medewerker_competentie_config:{success_count} OK, {error_count} fouten")
            return error_count == 0

        except Exception as e:
            print(f"🔥 Fatale fout bij opslaan competenties:{e}")
            traceback.print_exc()
            return False
        
    
    def _normalize_certname(self, cert_name: str) -> str:
        """
        V20: Synchronized Normalizer voor de database laag.
        Zorgt dat 'HS' en 'Hoogspanning' dezelfde technische sleutel krijgen.
        """
        if not cert_name or pd.isna(cert_name):
            return ""
        import re
        
        # 1. Standaardiseer tekst
        s = str(cert_name).strip().upper()
        
        # 2. HS/LS afkortingen gelijk trekken (Cruciaal voor ZA1222)
        s = re.sub(r'\bHS\b', 'HOOGSPANNING', s)
        s = re.sub(r'\bLS\b', 'LAAGSPANNING', s)
        s = re.sub(r'\bBT\b', 'LAAGSPANNING', s)
        s = re.sub(r'\bHT\b', 'HOOGSPANNING', s)
        
        # 3. Naar technische kleine-letter-sleutel
        s = s.lower()
        s = re.sub(r'[^a-z0-9]', '', s)
        return s

   
    def get_todo_planner(self, costcenter: str = None) -> pd.DataFrame:
        """
        Haalt de inhoud van de TODO tabel op.
        ALS costcenter is opgegeven, haalt hij ALLEEN dat costcenter op (Server-side filter).
        """
        import pandas as pd
        from sqlalchemy import text

        if not self.engine:
            return pd.DataFrame()

        try:
            # 🛡️ DE CRUCIALE UPDATE: FILTER IN DE QUERY MET SQLALCHEMY TEXT
            if costcenter:
                # We filteren direct in SQL. Dit voorkomt dat we data van andere afdelingen ophalen.
                # Gebruik named parameters (:cc) voor veiligheid en duidelijkheid
                query = text("SELECT * FROM dbo.TM_TodoPlanner WHERE CostCenter = :cc")
                params = {"cc": str(costcenter).strip()}
                print(f"   🕵️ SQL: Ophalen taken voor specifiek CostCenter: {costcenter}")
            else:
                # Fallback: haal alles op (alleen als geen CC is opgegeven)
                query = text("SELECT * FROM dbo.TM_TodoPlanner")
                params = {}
                print("   ⚠️ SQL: Ophalen ALLE taken (geen filter opgegeven)")

            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params=params)

            # Datum conversie
            date_cols = ["Ingeschreven_Datum", "ExpiryDate", "CreatedAt", "LastUpdatedAt"]
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            print(f"   ✅ SQL: {len(df)} taken opgehaald.")
            return df

        except Exception as e:
            print(f"   ❌ SQL Fout bij ophalen TodoPlanner: {e}")
            return pd.DataFrame()
 
    def save_todo_planner(self, df: pd.DataFrame) -> (bool, dict):
        """
        V51-FINAL: Slimme Opslag.
        Repareert automatisch ontbrekende CostCenters (NaN) door te kijken naar de context.
        Daarna pas de Firewall.
        """
        import pandas as pd
        import uuid
        from sqlalchemy import text
        from sqlalchemy.types import String

        mapping = {} 

        # 1. Basis checks
        if self.engine is None or df is None or df.empty:
            return True, mapping

        try:
            # --- A. DATA PREPARATIE ---
            df_save = df.copy()
            
            # 🛡️ REPARATIE: Vul ontbrekende CostCenters in
            # Stap 1: Zoek het 'dominante' costcenter in deze set
            active_cc = None
            valid_ccs = df_save["CostCenter"].dropna().astype(str).str.strip()
            valid_ccs = valid_ccs[~valid_ccs.isin(['nan', 'none', '', 'None'])]
            
            if not valid_ccs.empty:
                # Pak de meest voorkomende (of gewoon de eerste)
                active_cc = valid_ccs.iloc[0] 
                
                # Stap 2: Vul de lege plekken (NaN/None) met dit costcenter
                # Dit repareert de 108 taken die 'vergeten' waren
                mask = (df_save["CostCenter"].isna()) | (df_save["CostCenter"].astype(str).str.strip() == "") | (df_save["CostCenter"].astype(str).str.lower() == "nan")
                if mask.any():
                    print(f"   🔧 AUTO-FIX: {mask.sum()} taken zonder afdeling toegewezen aan '{active_cc}'.")
                    df_save.loc[mask, "CostCenter"] = active_cc
            else:
                print("   ⚠️ WAARSCHUWING: Geen enkel geldig CostCenter gevonden in de opslag-batch.")
                # We kunnen niet veilig opslaan als we de afdeling niet weten
                return False, mapping

            # --- EINDE REPARATIE ---

            # Nu pas de firewall filteren (voor de zekerheid)
            df_save = df_save[df_save["CostCenter"].astype(str).str.strip() == active_cc].copy()

            print(f"   💾 SQL SAVE: Bezig met opslaan voor afdeling '{active_cc}' ({len(df_save)} taken)...")

            # Cleaning van staffGID
            if "staffGID" in df_save.columns:
                df_save["staffGID"] = df_save["staffGID"].astype(str).str.strip()
                df_save["MedewerkerID"] = df_save["staffGID"] 
            
            # Zorg voor technische ID
            if "_SrcRowId" not in df_save.columns:
                df_save["_SrcRowId"] = [str(uuid.uuid4()) for _ in range(len(df_save))]
            df_save["_SrcRowId"] = df_save["_SrcRowId"].astype(str)

            # Integers cleanen
            for col in ["DaysUntilExpiry", "Geldigheid_maanden"]:
                if col in df_save.columns:
                    df_save[col] = pd.to_numeric(df_save[col], errors='coerce').astype('Int64')

            # --- B. TIJDELIJKE TABEL VULLEN ---
            df_save.to_sql("temp_todo_sync", self.engine, if_exists="replace", index=False, 
                           dtype={'_SrcRowId': String(50), 'staffGID': String(50), 'CostCenter': String(50)})

            # --- C. DE QUERY (MET FIREWALL) ---
            sql_query = text(f"""
                SET NOCOUNT ON;

                -- 1. DELETE (Alleen binnen de muren van active_cc)
                DELETE FROM dbo.TM_TodoPlanner 
                WHERE CostCenter = '{active_cc}' 
                AND NOT EXISTS (
                    SELECT 1 FROM dbo.temp_todo_sync tmp 
                    WHERE tmp.staffGID = dbo.TM_TodoPlanner.staffGID 
                    AND tmp.CertName_norm = dbo.TM_TodoPlanner.CertName_norm
                    AND tmp.TaskType = dbo.TM_TodoPlanner.TaskType
                );

                -- 2. MERGE
                MERGE INTO dbo.TM_TodoPlanner AS target
                USING (
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY staffGID, CertName_norm, TaskType ORDER BY LastUpdatedAt DESC
                        ) as rn FROM dbo.temp_todo_sync
                    ) x WHERE x.rn = 1
                ) AS src
                ON (target.staffGID = src.staffGID 
                    AND target.CertName_norm = src.CertName_norm 
                    AND target.TaskType = src.TaskType)
                
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.MedewerkerNaam = src.MedewerkerNaam,
                        target.CostCenter = src.CostCenter,
                        target.Status = src.Status,
                        target.Status_Detail = src.Status_Detail,
                        target.LastUpdatedAt = GETDATE(),
                        target.ExpiryDate = src.ExpiryDate,
                        target.DaysUntilExpiry = src.DaysUntilExpiry,
                        target.Nodig = src.Nodig,
                        -- 🔧 FIX: Behoud bestaande datum/locatie als nieuwe waarde NULL is
                        target.Ingeschreven_Datum = COALESCE(src.Ingeschreven_Datum, target.Ingeschreven_Datum),
                        target.Ingeschreven_Locatie = COALESCE(NULLIF(src.Ingeschreven_Locatie, ''), target.Ingeschreven_Locatie)

                WHEN NOT MATCHED THEN
                    INSERT (staffGID, staffSAPNR, MedewerkerID, MedewerkerNaam, CostCenter, 
                            CertName, CertName_norm, TaskType, Status, Status_Detail, 
                            Nodig, CreatedAt, LastUpdatedAt, CreatedBy, ExpiryDate, DaysUntilExpiry) 
                    VALUES (src.staffGID, src.staffSAPNR, src.staffGID, src.MedewerkerNaam, 
                            '{active_cc}', src.CertName, src.CertName_norm, src.TaskType, 
                            src.Status, src.Status_Detail, src.Nodig, GETDATE(), GETDATE(), 
                            src.CreatedBy, src.ExpiryDate, src.DaysUntilExpiry)
                
                OUTPUT inserted.TaskID, src._SrcRowId;
            """)

            with self.engine.begin() as conn:
                result = conn.execute(sql_query)
                for row in result:
                    if len(row) >= 2 and row[1]:
                        mapping[str(row[1])] = int(row[0]) if row[0] is not None else None
                
                conn.execute(text("IF OBJECT_ID('dbo.temp_todo_sync') IS NOT NULL DROP TABLE dbo.temp_todo_sync"))

            print(f"   ✅ SQL: Opslaan geslaagd voor {len(df_save)} taken in {active_cc}.")
            return True, mapping

        except Exception as e:
            print(f"   ❌ Fout in save_todo_planner: {e}")
            try:
                with self.engine.begin() as conn:
                    conn.execute(text("IF OBJECT_ID('dbo.temp_todo_sync') IS NOT NULL DROP TABLE dbo.temp_todo_sync"))
            except: pass
            return False, mapping
    def get_certificaat_mapping(self) -> pd.DataFrame:
        """
        Haalt de vertaaltabel op uit SQL: TM_CertificaatMapping.
        """
        import pandas as pd

        if not getattr(self, "engine", None):
            return pd.DataFrame()

        try:
            # Lees de tabel uit
            query = "SELECT OrigineleNaam, VertaaldeNaam FROM dbo.TM_CertificaatMapping"
            df = pd.read_sql(query, self.engine)
            
            # Zorg dat de kolomnamen matchen met wat DataStore verwacht
            # (DataStore zoekt vaak naar 'OrigineleNaam' en 'VertaaldeNaam')
            print(f"✅ SQL: {len(df)} mappings opgehaald.")
            return df

        except Exception as e:
            print(f"ℹ️ SQL Info: Kon mapping niet ophalen ({e})")
            return pd.DataFrame()

    # =================================================================
    #  MASTER DATA SCHRIJVEN (TOEVOEGEN AAN SQLServerTrainingManager)
    # =================================================================

    # def add_master_certificate(self, cert_name: str) -> bool:
        # """Voegt een nieuw certificaat toe aan TM_MasterCertificaten."""
        # if not self.engine or not cert_name: return False
        # try:
            # # Check of hij al bestaat
            # check = "SELECT Count(*) FROM dbo.TM_MasterCertificaten WHERE CertName = ?"
            # import pandas as pd
            # exists = pd.read_sql(check, self.engine, params=(cert_name,)).iloc[0, 0] > 0
            
            # if exists:
                # print(f"⚠️ Master Certificaat '{cert_name}' bestaat al.")
                # return True

            # # Toevoegen
            # from sqlalchemy import text
            # with self.engine.begin() as conn:
                # conn.execute(text("""
                    # INSERT INTO dbo.TM_MasterCertificaten (CertName, Active, Categorie, StrategischBelangrijk)
                    # VALUES (:name, 1, 'Auto-Created', 0)
                # """), {"name": cert_name})
            # print(f"✅ Master Certificaat aangemaakt: {cert_name}")
            # return True
        # except Exception as e:
            # print(f"❌ Fout bij aanmaken master certificaat: {e}")
            # return False
    
    # def add_master_competence(self, comp_name: str) -> bool:
        # """Voegt een nieuwe competentie toe aan TM_MasterCompetenties."""
        # if not self.engine or not comp_name: return False
        # try:
            # check = "SELECT Count(*) FROM dbo.TM_MasterCompetenties WHERE Competence = ?"
            # import pandas as pd
            # exists = pd.read_sql(check, self.engine, params=(comp_name,)).iloc[0, 0] > 0
            
            # if exists: return True

            # from sqlalchemy import text
            # with self.engine.begin() as conn:
                # conn.execute(text("""
                    # INSERT INTO dbo.TM_MasterCompetenties (Competence, Active, Categorie, StrategischBelangrijk)
                    # VALUES (:name, 1, 'Auto-Created', 0)
                # """), {"name": comp_name})
            # print(f"✅ Master Competentie aangemaakt: {comp_name}")
            # return True
        # except Exception as e:
            # print(f"❌ Fout bij aanmaken master competentie: {e}")
            # return False
    def add_master_certificate(self, cert_name:  str) -> bool:
        """Voegt een nieuw certificaat toe aan TM_MasterCertificaten."""
        from sqlalchemy import text
        import re
        
        if not self.engine or not cert_name:
            return False
        
        cert_name = cert_name.strip()
        cert_norm = re.sub(r'[^a-z0-9]', '', cert_name.lower())
        
        try:
            with self.engine.begin() as conn:
                # Check of hij al bestaat
                check_sql = text("""
                    SELECT COUNT(*) as cnt FROM dbo.TM_MasterCertificaten 
                    WHERE CertName = :name OR CertName_norm = :norm
                """)
                result = conn.execute(check_sql, {"name":cert_name, "norm":cert_norm})
                row = result.fetchone()
                
                if row and row[0] > 0:
                    print(f"⚠️ Master Certificaat '{cert_name}' bestaat al.")
                    return True

                # Toevoegen
                insert_sql = text("""
                    INSERT INTO dbo.TM_MasterCertificaten 
                    (CertName, CertName_norm, Active, Categorie, StrategischBelangrijk, 
                     LaatsteWijziging, GewijzigdDoor)
                    VALUES (:name, :norm, 1, 'Auto-Created', 0, GETDATE(), 'TMS App')
                """)
                conn.execute(insert_sql, {"name":cert_name, "norm":cert_norm})
                
            print(f"✅ Master Certificaat aangemaakt: {cert_name}")
            return True
            
        except Exception as e: 
            print(f"❌ Fout bij aanmaken master certificaat: {e}")
            import traceback
            traceback.print_exc()
            return False


    def add_master_competence(self, comp_name: str) -> bool:
        """Voegt een nieuwe competentie toe aan TM_MasterCompetenties."""
        from sqlalchemy import text
        import re
        
        if not self.engine or not comp_name:
            return False
        
        comp_name = comp_name.strip()
        comp_norm = re.sub(r'[^a-z0-9]', '', comp_name.lower())
        
        try:
            with self.engine.begin() as conn:
                # Check of hij al bestaat
                check_sql = text("""
                    SELECT COUNT(*) as cnt FROM dbo.TM_MasterCompetenties 
                    WHERE Competence = :name OR Competence_norm = :norm
                """)
                result = conn.execute(check_sql, {"name":comp_name, "norm":comp_norm})
                row = result.fetchone()
                
                if row and row[0] > 0:
                    print(f"⚠️ Master Competentie '{comp_name}' bestaat al.")
                    return True

                # Toevoegen
                insert_sql = text("""
                    INSERT INTO dbo.TM_MasterCompetenties 
                    (Competence, Competence_norm, Active, Categorie, StrategischBelangrijk, 
                     LaatsteWijziging, GewijzigdDoor)
                    VALUES (:name, :norm, 1, 'Auto-Created', 0, GETDATE(), 'TMS App')
                """)
                conn.execute(insert_sql, {"name":comp_name, "norm":comp_norm})
                
            print(f"✅ Master Competentie aangemaakt:  {comp_name}")
            return True
            
        except Exception as e:
            print(f"❌ Fout bij aanmaken master competentie: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def add_medewerker_config(self, staff_id: str, cert_name: str, nodig: bool = True):
        """
        Voegt een certificaat toe aan de config tabel (UPSERT).
        Dit is de SQL Server implementatie - doet directe database operaties.
        """
        from datetime import datetime
        from sqlalchemy import text
        
        print(f"   ➕ add_medewerker_config: {cert_name} voor {staff_id}")
        
        try:
            cert_norm = self._normalize_certname(cert_name)
            now = datetime.now()
            
            # Check of record al bestaat
            check_query = text("""
                SELECT ConfigID FROM TM_MedewerkerCertificaatConfig
                WHERE staffGID = :staff_id AND CertName_norm = :cert_norm
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(check_query, {"staff_id": staff_id, "cert_norm": cert_norm})
                existing = result.fetchone()
                
                if existing:
                    # UPDATE bestaand record
                    update_query = text("""
                        UPDATE TM_MedewerkerCertificaatConfig
                        SET Nodig = :nodig, LaatsteWijziging = :now
                        WHERE staffGID = :staff_id AND CertName_norm = :cert_norm
                    """)
                    conn.execute(update_query, {
                        "nodig":  1 if nodig else 0,
                        "now": now,
                        "staff_id": staff_id,
                        "cert_norm": cert_norm
                    })
                    conn.commit()
                    print(f"   ✏️ UPDATE: {cert_name} voor {staff_id}")
                else: 
                    # INSERT nieuw record
                    insert_query = text("""
                        INSERT INTO TM_MedewerkerCertificaatConfig
                        (staffGID, CertName, CertName_norm, Nodig, Strategisch, LaatsteWijziging)
                        VALUES (:staff_id, :cert_name, :cert_norm, :nodig, 0, :now)
                    """)
                    conn.execute(insert_query, {
                        "staff_id":  staff_id,
                        "cert_name": cert_name,
                        "cert_norm": cert_norm,
                        "nodig":  1 if nodig else 0,
                        "now": now
                    })
                    conn.commit()
                    print(f"   ➕ INSERT: {cert_name} voor {staff_id}")
            
            return True
            
        except Exception as e: 
            print(f"   ❌ Fout bij add_medewerker_config: {e}")
            import traceback
            traceback.print_exc()
            return False

    # Alias voor backwards compatibility
    add_medewerker_certificaat_config = add_medewerker_config
    
    def delete_medewerker_certificaat_config(self, staff_gid:  str, cert_name: str) -> bool:
        """
        Verwijdert een certificaat config regel uit SQL Server.
        
        Args:
            staff_gid: De staffGID van de medewerker
            cert_name: De naam van het certificaat
            
        Returns: 
            True als succesvol, False bij fout
        """
        from sqlalchemy import text
        
        if not staff_gid or not cert_name: 
            print(f"   ⚠️ DELETE certificaat: staffGID of CertName ontbreekt")
            return False
        
        if getattr(self, "engine", None) is None:
            print("   ❌ DELETE certificaat: Geen SQL engine beschikbaar")
            return False
        
        print(f"   🗑️ DELETE certificaat: {cert_name} voor {staff_gid}")
        
        try:
            with self.engine.begin() as conn:
                # Eerst checken of het bestaat
                check_sql = text("""
                    SELECT ConfigID FROM dbo.TM_MedewerkerCertificaatConfig
                    WHERE staffGID = :gid AND CertName = :cert
                """)
                result = conn.execute(check_sql, {"gid": staff_gid, "cert": cert_name})
                existing = result.fetchone()
                
                if not existing:
                    print(f"   ⚠️ DELETE certificaat: Geen match gevonden voor {cert_name} / {staff_gid}")
                    return False
                
                # Verwijderen
                delete_sql = text("""
                    DELETE FROM dbo.TM_MedewerkerCertificaatConfig
                    WHERE staffGID = :gid AND CertName = :cert
                """)
                conn.execute(delete_sql, {"gid": staff_gid, "cert": cert_name})
                
                print(f"   ✅ DELETE certificaat: {cert_name} voor {staff_gid} VERWIJDERD")
                return True
                
        except Exception as e:
            print(f"   ❌ DELETE certificaat fout: {e}")
            import traceback
            traceback.print_exc()
            return False


    def delete_medewerker_competentie_config(self, staff_gid: str, competence:  str) -> bool:
        """
        Verwijdert een competentie config regel uit SQL Server. 
        
        Args:
            staff_gid: De staffGID van de medewerker
            competence: De naam van de competentie/vaardigheid
            
        Returns:
            True als succesvol, False bij fout
        """
        from sqlalchemy import text
        
        if not staff_gid or not competence: 
            print(f"   ⚠️ DELETE competentie: staffGID of Competence ontbreekt")
            return False
        
        if getattr(self, "engine", None) is None:
            print("   ❌ DELETE competentie:  Geen SQL engine beschikbaar")
            return False
        
        print(f"   🗑️ DELETE competentie: {competence} voor {staff_gid}")
        
        try:
            with self.engine. begin() as conn:
                # Eerst checken of het bestaat
                check_sql = text("""
                    SELECT ConfigID FROM dbo.TM_MedewerkerCompetentieConfig
                    WHERE staffGID = :gid AND Competence = :comp
                """)
                result = conn.execute(check_sql, {"gid": staff_gid, "comp": competence})
                existing = result.fetchone()
                
                if not existing:
                    print(f"   ⚠️ DELETE competentie:  Geen match gevonden voor {competence} / {staff_gid}")
                    return False
                
                # Verwijderen
                delete_sql = text("""
                    DELETE FROM dbo.TM_MedewerkerCompetentieConfig
                    WHERE staffGID = :gid AND Competence = :comp
                """)
                conn.execute(delete_sql, {"gid": staff_gid, "comp": competence})
                
                print(f"   ✅ DELETE competentie: {competence} voor {staff_gid} VERWIJDERD")
                return True
                
        except Exception as e:
            print(f"   ❌ DELETE competentie fout: {e}")
            import traceback
            traceback.print_exc()
            return False
            
    def delete_todo_task(self, staff_gid: str, cert_name_norm: str) -> bool:
        """
        Verwijdert een taak uit de TM_TodoPlanner.
        
        Args:
            staff_gid: De staffGID van de medewerker
            cert_name_norm: De genormaliseerde naam van het certificaat/vaardigheid
            
        Returns: 
            True als succesvol, False bij fout
        """
        from sqlalchemy import text
        
        if not staff_gid or not cert_name_norm: 
            print(f"   ⚠️ DELETE todo: staffGID of CertName_norm ontbreekt")
            return False
        
        if getattr(self, "engine", None) is None:
            print("   ❌ DELETE todo: Geen SQL engine beschikbaar")
            return False
        
        print(f"   🗑️ DELETE todo: {cert_name_norm} voor {staff_gid}")
        
        try:
            with self.engine.begin() as conn:
                delete_sql = text("""
                    DELETE FROM dbo.TM_TodoPlanner
                    WHERE staffGID = :gid AND CertName_norm = :norm
                """)
                result = conn.execute(delete_sql, {"gid": staff_gid, "norm": cert_name_norm})
                
                rows_deleted = result. rowcount
                if rows_deleted > 0:
                    print(f"   ✅ DELETE todo: {rows_deleted} taak/taken verwijderd")
                else:
                    print(f"   ℹ️ DELETE todo: Geen taken gevonden om te verwijderen")
                
                return True
                
        except Exception as e: 
            print(f"   ❌ DELETE todo fout:  {e}")
            import traceback
            traceback.print_exc()
            return False