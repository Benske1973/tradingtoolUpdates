# ===============================================================
# Training Management System v10 - PyQt6 (FIXED VERSION)
# Dashboard + Medewerkerbeheer + Planner/To-do (automatisch + zwevend)
# ===============================================================

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
from xaurum.db.staff_manager import SQLServerStaffManager
from xaurum.db.training_manager import SQLServerTrainingManager

# =========================================================
# GLOBALE HELPERS
# =========================================================


class DataStore:
    def __init__(self):
        # ═══════════════════════════════════════════════════════════
        # BESTAANDE DATASTRUCTUREN
        # ═══════════════════════════════════════════════════════════
        
        self.df: Dict[str, pd.DataFrame] = {}
        self.master_comp_req: pd.DataFrame = pd.DataFrame()
        self.master_comp_all: pd.DataFrame = pd.DataFrame()
        self.errors: List[str] = []
        self.active_costcenter: Optional[str] = None
        self.full_access: bool = False
        self.training_catalog: pd.DataFrame = pd.DataFrame()
        self.base_dir = BASE_DIR
        self.config_dir = CONFIG_DIR

        self.master_cert_all = pd.DataFrame()
        self.master_cert_req = pd.DataFrame()
        self.master_comp_all = pd.DataFrame()
        self.master_comp_req = pd.DataFrame()
        self.config_cert = pd.DataFrame()
        self.config_comp = pd.DataFrame()

        # Helper sets voor zoekfunctie
        self.all_cert_names: set = set()
        self.all_competence_names: set = set()

        # Backwards-compat/UI helpers
        self._cert_display_map: Dict[str, str] = {}
        self._cert_display_map_built: bool = False
        
        # Track sync statistics
        self.last_sync_merge_count: int = 0
        
        # 🆕 VERTALINGEN DICTIONARY (Voor Frans -> Nederlands)
        self.translation_dict: Dict[str, str] = {} 

        # ═══════════════════════════════════════════════════════════
        # 🆕 SQL SERVER CONFIGURATIE (V11 - VOLLEDIG)
        # ═══════════════════════════════════════════════════════════
        
        # SQL Server toggles
        self.USE_SQL_FOR_STAFF = True       # Staff uit SQL
        self.USE_SQL_FOR_CONFIG = True      # Config uit SQL
        self.USE_SQL_FOR_TODO = True        # Todo uit SQL
        self.USE_SQL_FOR_MASTER = True      # Master data uit SQL
        
        # SQL Server instellingen
        self.SQL_CONFIG = {
            "server": "SHRDSQLDEVVM01",
            "database": "Operations_support_portal"
        }

        # 🆕 ENGINE AANMAKEN (Voor directe queries zoals vertalingen)
        from sqlalchemy import create_engine
        try:
            conn_str = (
                f"mssql+pyodbc:///?odbc_connect="
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.SQL_CONFIG['server']};"
                f"DATABASE={self.SQL_CONFIG['database']};"
                f"Trusted_Connection=yes;"
            )
            self.engine = create_engine(conn_str)
            print("🔌 DataStore SQL Engine gestart.")
        except Exception as e:
            print(f"⚠️ Kon SQL Engine niet starten in DataStore: {e}")
            self.engine = None
        
        # Maak SQL managers placeholders
        self.sql_staff_manager: Optional[SQLServerStaffManager] = None
        self.sql_training_manager: Optional[SQLServerTrainingManager] = None
        
        # ══════════════════════════════════════════════════════════════
        # Initialiseer Staff Manager
        # ══════════════════════════════════════════════════════════════
        if self.USE_SQL_FOR_STAFF:
            try:
                print("🔌 Initialiseren SQL Staff Manager...")
                self.sql_staff_manager = SQLServerStaffManager(
                    server=self.SQL_CONFIG["server"],
                    database=self.SQL_CONFIG["database"],
                    table="dbo.tblSTAFF"
                )
                
                if not self.sql_staff_manager.is_available():
                    print("⚠️ SQL Staff Manager niet beschikbaar")
                    self.sql_staff_manager = None
                else:
                    print("✅ SQL Staff Manager gereed")
                    
            except Exception as e:
                print(f"⚠️ SQL Staff Manager initialisatie fout: {e}")
                self.sql_staff_manager = None
        
        # ══════════════════════════════════════════════════════════════
        # Initialiseer Training Manager
        # ══════════════════════════════════════════════════════════════
        if self.USE_SQL_FOR_CONFIG or self.USE_SQL_FOR_TODO or self.USE_SQL_FOR_MASTER:
            try:
                print("🔌 Initialiseren SQL Training Manager...")
                self.sql_training_manager = SQLServerTrainingManager(
                    server=self.SQL_CONFIG["server"],
                    database=self.SQL_CONFIG["database"]
                )
                
                if not self.sql_training_manager.is_available():
                    print("⚠️ SQL Training Manager niet beschikbaar")
                    self.sql_training_manager = None
                else:
                    print("✅ SQL Training Manager gereed")
                    
            except Exception as e:
                print(f"⚠️ SQL Training Manager initialisatie fout: {e}")
                self.sql_training_manager = None
        
        print() 

        # 🆕 DIRECT VERTALINGEN LADEN
        self.load_translations()

        
    def normalize_certname(self, name):
        import re
        import pandas as pd
        if name is None or pd.isna(name): return ""
        
        s = str(name).strip()
        
        # 1. Gebruik Mapping (Veilige manier)
        # We gebruiken getattr zodat we nooit een crash forceren als de dict niet bestaat
        mapping = getattr(self, "translation_dict", None)
        if mapping and isinstance(mapping, dict):
            if s in mapping:
                s = mapping[s]

        # 2. Uniforme termen (Eerst alles naar Upper om makkelijker te vervangen)
        s = s.upper()
        
        # Voorkom dat 'BASSE TENSION' half vervangen wordt door 'LS' regels
        s = s.replace("BASSE TENSION", "LAAGSPANNING")
        s = s.replace("HAUTE TENSION", "HOOGSPANNING")
        s = s.replace("MANOEUVRES", "SCHAKELEN")
        
        # Afkortingen
        s = re.sub(r'\b(LS|BT)\b', 'LAAGSPANNING', s)
        s = re.sub(r'\b(HS|HT)\b', 'HOOGSPANNING', s)
        
        # 3. Technische sleutel (De match-sleutel voor SQL)
        s = s.lower().replace("equans", "").replace("-", "").replace("_", "").replace(" ", "")
        s = re.sub(r'[^a-z0-9]', '', s)
        
        return s
 
    def load_translations(self):
        """
        Laadt de vertaaltabel (TM_NaamMapping) uit SQL (of Excel) in het geheugen.
        """
        # Reset
        self.translation_dict = {}
        df_map = pd.DataFrame()

        # 1. Probeer SQL (Gebruik de BESTAANDE tabel)
        if self.engine:
            try:
                # 👇 AANGEPAST: Juiste tabelnaam
                query = "SELECT OrigineleNaam, VertaaldeNaam FROM dbo.TM_NaamMapping"
                df_map = pd.read_sql(query, self.engine)
                print(f"✅ SQL Mapping geladen: {len(df_map)} regels uit TM_NaamMapping")
            except Exception as e:
                print(f"ℹ️ SQL Mapping info: {e}")

        # 2. Fallback naar Excel als SQL leeg is of faalde
        if df_map.empty:
            try:
                import os
                # Probeer pad te vinden
                base = getattr(self, "base_dir", getattr(self, "base_path", os.getcwd()))
                excel_path = os.path.join(base, "config", "Mapping.xlsx")
                
                if os.path.exists(excel_path):
                    df_map = pd.read_excel(excel_path)
                    print(f"📂 Excel Mapping geladen: {len(df_map)} regels")
            except Exception as e:
                print(f"⚠️ Excel Mapping fout: {e}")

        # 3. Verwerken naar Dictionary en DataFrame
        if not df_map.empty:
            # Kolomnamen normaliseren
            src_col = next((c for c in ["OrigineleNaam", "Frans", "OriginalName"] if c in df_map.columns), df_map.columns[0])
            dst_col = next((c for c in ["VertaaldeNaam", "Nederlands", "DutchName"] if c in df_map.columns), df_map.columns[1])

            # Opslaan in DataStore (belangrijk voor clean_sql_config_names)
            self.df["mapping_cert"] = df_map

            # Opslaan in Dictionary (belangrijk voor normalize_certname)
            self.translation_dict = pd.Series(
                df_map[dst_col].astype(str).str.strip().values, 
                index=df_map[src_col].astype(str).str.strip().values
            ).to_dict()
            
            print(f"✅ Vertalingen actief: {len(self.translation_dict)} termen.")
        else:
            print("ℹ️ Geen vertalingen gevonden (SQL en Excel leeg).")
    
    def _load_and_translate_excel(self, file_path):
        """
        Hulpfunctie: Leest een Excel/CSV en vertaalt direct alle bekende Franse termen.
        Vervangt pd.read_excel in je load functies.
        """
        if not file_path or not os.path.exists(file_path):
            return pd.DataFrame()

        try:
            # Check extensie
            if str(file_path).endswith('.csv'):
                df = pd.read_csv(file_path, sep=None, engine='python')
            else:
                df = pd.read_excel(file_path)

            # 1. Kolommen opschonen
            df.columns = df.columns.str.strip()
            
            # 2. Zoek naar kolommen die we moeten vertalen
            target_cols = ['CertName', 'Certificaat', 'Competentie', 'Formation', 'Compétence', 'Certificat']
            
            for col in target_cols:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
                    
                    # DE MAGIE: Vervang Franse termen door Nederlandse
                    if self.translation_dict:
                        df[col] = df[col].replace(self.translation_dict)
                        
                    # Standaardiseer kolomnaam naar 'CertName' (handig voor later)
                    if col != 'CertName' and col in ['Certificat', 'Formation', 'Certificaat']:
                         df['CertName'] = df[col]

            print(f"✅ Bestand geladen & vertaald: {os.path.basename(file_path)}")
            return df

        except Exception as e:
            print(f"❌ Fout bij laden bestand {file_path}: {e}")
            return pd.DataFrame()
   
    # ══════════════════════════════════════════════════════════════
    # 🆕 HIER INVOEGEN: load_staff_only()
    # ══════════════════════════════════════════════════════════════
    
    def load_staff_only(self) -> bool:
        """
        Laadt ALLEEN staff data (voor costcenter selectie).
        """
        self.errors = []
        
        print("\n" + "="*60)
        print("📊 STAP 1: STAFF LADEN (voor costcenter selectie)")
        print("="*60)
        
        try:
            if self.USE_SQL_FOR_STAFF and self.sql_staff_manager:
                print("   → Bron: SQL Server")
                staff = self.sql_staff_manager.get_all_staff()
                
                if staff.empty:
                    print("   ❌ SQL gaf geen data")
                    self.errors.append("Geen staff data uit SQL")
                    return False
                else:
                    print(f"   ✅ SQL: {len(staff)} medewerkers geladen")
            else:
                print("   ❌ SQL niet beschikbaar")
                self.errors.append("SQL niet beschikbaar voor staff")
                return False
            
            # Filter op actieve medewerkers
            status_col = "staffSTAFFSTATUSID"
            if status_col in staff.columns:
                before = len(staff)
                status_num = pd.to_numeric(staff[status_col], errors="coerce")
                staff = staff[status_num == 1].copy()
                after = len(staff)
                if before != after:
                    print(f"   → Filter: {after} actief (was {before})")
            
            # FullName aanmaken
            if "staffFIRSTNAME" in staff.columns and "staffLASTNAME" in staff.columns:
                staff["FullName"] = (
                    staff["staffLASTNAME"].astype(str).str.strip() + ", " + 
                    staff["staffFIRSTNAME"].astype(str).str.strip()
                )
            
            # IDs formatteren
            if "staffGID" in staff.columns:
                staff["staffGID"] = staff["staffGID"].astype(str).str.strip()
            
            if "staffSAPNR" in staff.columns:
                staff["staffSAPNR"] = staff["staffSAPNR"].apply(self._normalize_sapnr)
            
            self.df["staff"] = staff
            print(f"   ✅ STAFF: {len(staff)} medewerkers geladen")
            return True
            
        except Exception as e:
            self.errors.append(f"Fout bij laden STAFF: {e}")
            print(f"   ❌ STAFF fout: {e}")
            import traceback
            traceback.print_exc()
            return False
 
    def add_unique_comment(self, text: str, addition: str) -> str:
        addition = addition.strip()
        if addition in text:
            return text
        if not text.endswith(" "):
            text += " "
        return text + addition

    def check_certnames_against_master(self):
        """
        Controleert of alle CertNames uit certificates en training_req 
        voorkomen in de master certificaten lijst.
        
        Returns:
            dict met 'certificates' en 'training_req' lijsten van ontbrekende namen
        """
        result = {
            "certificates": [],
            "training_req": [],
        }

        # 🔧 FIX: Gebruik master_cert_all (niet master_comp_all!)
        if self.master_cert_all.empty or "CertName" not in self.master_cert_all.columns:
            print("⚠️ Master certificaten lijst is leeg of heeft geen CertName kolom")
            return result

        norm = self.normalize_certname
        master_set = set(
            self.master_cert_all["CertName"]
            .dropna()
            .astype(str)
            .map(norm)
        )

        print(f"\n🔍 Controleren tegen {len(master_set)} master certificaten...")

        for key in ("certificates", "training_req"):
            df = self.df.get(key, pd.DataFrame())
            missing: list[str] = []

            if df is None or df.empty or "CertName" not in df.columns:
                result[key] = missing
                continue

            orig = df["CertName"].dropna().astype(str)
            normed = orig.map(norm)

            for cert_norm in sorted(normed.unique()):
                if not cert_norm:
                    continue
                if cert_norm not in master_set:
                    sample = orig[normed == cert_norm].iloc[0]
                    missing.append(str(sample))

            result[key] = sorted(set(missing))
            
            if missing:
                print(f"   ⚠️ {key}: {len(missing)} ontbrekende certificaten")

        return result

    def add_missing_certnames_to_master(self, missing_dict: dict) -> int:
        """
        Voegt ontbrekende certificaatnamen toe aan master (via SQL).
        
        Args:
            missing_dict: Dict met 'certificates' en 'training_req' lijsten
            
        Returns:
            int: Aantal toegevoegde certificaten
        """
        missing_cert = missing_dict.get("certificates") or []
        missing_req = missing_dict.get("training_req") or []
        to_add_raw = sorted(set(missing_cert + missing_req))

        if not to_add_raw:
            print("ℹ️ Geen certificaten om toe te voegen")
            return 0

        print(f"\n{'='*60}")
        print(f"📜 CERTIFICATEN TOEVOEGEN AAN MASTER")
        print(f"{'='*60}")
        print(f"   Te verwerken: {len(to_add_raw)} unieke certificaten")

        # ═══════════════════════════════════════════════════════════
        # SQL VERSIE
        # ═══════════════════════════════════════════════════════════
        if self.USE_SQL_FOR_MASTER and self.sql_training_manager:
            added = self.sql_training_manager.add_master_certificaten(to_add_raw)
            
            if added > 0:
                # Herlaad master data uit SQL
                print("\n🔄 Master certificaten herladen uit SQL...")
                master_cert = self.sql_training_manager.get_master_certificaten()
                
                if not master_cert.empty and "CertName" in master_cert.columns:
                    self.master_cert_all = (
                        master_cert[["CertName"]]
                        .drop_duplicates()
                        .sort_values("CertName")
                        .reset_index(drop=True)
                    )
                    
                    # Update helper set
                    self.all_cert_names = set(
                        self.master_cert_all["CertName"].dropna().astype(str).unique()
                    )
                    
                    print(f"   ✅ Master certificaten herladen: {len(self.master_cert_all)} totaal")
            
            print(f"{'='*60}\n")
            return added
        else:
            print("❌ SQL niet beschikbaar voor master certificaten")
            self.errors.append("SQL niet beschikbaar - kon certificaten niet toevoegen aan master")
            return 0
    
    def check_competences_against_master(self):
        """
        Controleert of alle Competence-namen uit competences 
        voorkomen in de master competenties lijst.
        
        Returns:
            dict met 'competences' lijst van ontbrekende namen
        """
        result = {"competences": []}

        if self.master_comp_all.empty or "Competence" not in self.master_comp_all.columns:
            print("⚠️ Master competenties lijst is leeg of heeft geen Competence kolom")
            return result

        norm = self.normalize_certname
        master_set = set(
            self.master_comp_all["Competence"]
            .dropna()
            .astype(str)
            .map(norm)
        )

        print(f"\n🔍 Controleren tegen {len(master_set)} master competenties...")

        df_comp = self.df.get("competences", pd.DataFrame())
        missing: list[str] = []

        if df_comp is None or df_comp.empty or "Competence" not in df_comp.columns:
            result["competences"] = missing
            return result

        orig = df_comp["Competence"].dropna().astype(str)
        normed = orig.map(norm)

        for comp_norm in sorted(normed.unique()):
            if not comp_norm:
                continue
            if comp_norm not in master_set:
                sample = orig[normed == comp_norm].iloc[0]
                missing.append(str(sample))

        result["competences"] = sorted(set(missing))
        
        if missing:
            print(f"   ⚠️ {len(missing)} ontbrekende competenties gevonden")

        return result

    def add_missing_competences_to_master(self, missing_dict: dict) -> int:
        """
        Voegt ontbrekende competenties toe aan master (via SQL).
        
        Args:
            missing_dict: Dict met 'competences' lijst
            
        Returns:
            int: Aantal toegevoegde competenties
        """
        to_add_raw = missing_dict.get("competences") or []

        if not to_add_raw:
            print("ℹ️ Geen competenties om toe te voegen")
            return 0

        print(f"\n{'='*60}")
        print(f"🎯 COMPETENTIES TOEVOEGEN AAN MASTER")
        print(f"{'='*60}")
        print(f"   Te verwerken: {len(to_add_raw)} unieke competenties")

        # ═══════════════════════════════════════════════════════════
        # SQL VERSIE (NIEUW!  - geen Excel meer)
        # ═══════════════════════════════════════════════════════════
        if self.USE_SQL_FOR_MASTER and self.sql_training_manager:
            added = self.sql_training_manager.add_master_competenties(to_add_raw)
            
            if added > 0:
                # Herlaad master data uit SQL
                print("\n🔄 Master competenties herladen uit SQL...")
                master_comp = self.sql_training_manager.get_master_competenties()
                
                if not master_comp.empty and "Competence" in master_comp.columns:
                    self.master_comp_all = (
                        master_comp[["Competence"]]
                        .drop_duplicates()
                        .sort_values("Competence")
                        .reset_index(drop=True)
                    )
                    
                    # Update helper set
                    self.all_competence_names = set(
                        self.master_comp_all["Competence"].dropna().astype(str).unique()
                    )
                    
                    print(f"   ✅ Master competenties herladen: {len(self.master_comp_all)} totaal")
            
            print(f"{'='*60}\n")
            return added
        else:
            print("❌ SQL niet beschikbaar voor master competenties")
            self.errors.append("SQL niet beschikbaar - kon competenties niet toevoegen aan master")
            return 0
    
    def get_id_column(self) -> Optional[str]:
        if "staff" in self.df and not self.df["staff"].empty:
            if "staffGID" in self.df["staff"].columns:
                return "staffGID"
            if "staffSAPNR" in self.df["staff"].columns:
                return "staffSAPNR"
        return None
    
    def calculate_expiry_date(self, issue_date, geldigheid_maanden):
        """
        Bereken expiry datum op basis van issue date en geldigheid.
        
        Args:
            issue_date: Start datum (pd.Timestamp of date)
            geldigheid_maanden: Aantal maanden geldig (int of None/NaN)
            
        Returns:
            pd.Timestamp: Expiry datum
        """
        # Check of onbeperkt geldig
        if pd.isna(geldigheid_maanden) or geldigheid_maanden is None:
            # Onbeperkt: gebruik 2099-12-31
            return pd.Timestamp('2099-12-31')
        
        # Normale berekening
        if pd.isna(issue_date):
            return pd.NaT
        
        try:
            return pd.to_datetime(issue_date) + pd.DateOffset(months=int(geldigheid_maanden))
        except Exception:
            return pd.NaT
    
    
    def is_certificate_valid(self, cert_name, expiry_date, geldigheid_maanden=None):
        """
        Check of certificaat nog geldig is.
        
        Args:
            cert_name: Naam van certificaat
            expiry_date: Expiry datum (kan 2099-12-31 zijn voor onbeperkt)
            geldigheid_maanden: Geldigheid in maanden (None = onbeperkt)
            
        Returns:
            tuple: (is_valid, status_message)
        """
        if pd.isna(expiry_date):
            return False, "Geen expiry datum"
        
        # Check of onbeperkt
        if pd.isna(geldigheid_maanden):
            return True, "♾️ Onbeperkt geldig"
        
        # Check expiry
        today = pd.Timestamp.now()
        
        # Check of datum in verre toekomst (onbeperkt)
        if expiry_date >= pd.Timestamp('2099-01-01'):
            return True, "♾️ Onbeperkt geldig"
        
        if expiry_date < today:
            days_expired = (today - expiry_date).days
            return False, f"❌ Verlopen ({days_expired} dagen geleden)"
        
        days_until_expiry = (expiry_date - today).days
        
        if days_until_expiry < 30:
            return True, f"⚠️ Verloopt binnen {days_until_expiry} dagen"
        elif days_until_expiry < 90:
            return True, f"⏰ Verloopt over {days_until_expiry} dagen"
        elif days_until_expiry < 180:
            return True, f"📅 Verloopt over ~{days_until_expiry // 30} maanden"
        else:
            return True, f"✅ Geldig tot {expiry_date.strftime('%d-%m-%Y')}"
    
    
    def should_create_renewal_task(self, expiry_date, geldigheid_maanden):
        """
        Bepaal of er een vernieuwingstaak aangemaakt moet worden.
        
        Args:
            expiry_date: Expiry datum
            geldigheid_maanden: Geldigheid (None = onbeperkt)
            
        Returns:
            bool: True als taak nodig is
        """
        # Geen taak voor onbeperkt geldige certificaten
        if pd.isna(geldigheid_maanden) or geldigheid_maanden is None:
            return False
        
        if pd.isna(expiry_date):
            return True  # Geen expiry = altijd taak aanmaken
        
        # Check of onbeperkt (2099-12-31)
        if expiry_date >= pd.Timestamp('2099-01-01'):
            return False  # Onbeperkt = geen taak
        
        # Taak aanmaken als expiry binnen 6 maanden
        today = pd.Timestamp.now()
        days_until_expiry = (expiry_date - today).days
        
        return days_until_expiry < 180  # 6 maanden


    def sync_cert_tasks(self):
        """
        ULTIMATE VERSION:  V17-DEBUG functionaliteit + V18 Afdelingsbeveiliging. 
        Zorgt voor correcte datums én voorkomt vervuiling van andere costcenters.
        
        V19-FIX: CostCenter lookup aangepast om te werken na kolom-hernoemen in load_all().
        V20-FIX: FutureWarning pd.concat opgelost. 
        """
        print("\n" + "="*60)
        print(f"🔄 sync_cert_tasks() - AFDELING: {self. active_costcenter}")
        print("="*60)
        
        # --- HULPFUNCTIE VOOR ZUIVERE INTEGERS ---
        def to_int_or_none(val):
            try:
                if pd.isna(val) or val == "" or str(val).strip().lower() == "nan":
                    return None
                return int(float(val))
            except: 
                return None

        # 1. LAAD DATA
        cfg = self.df. get("config_cert", pd.DataFrame())
        if cfg.empty:
            cfg = self.df.get("config", pd.DataFrame())
            
        staff = self.df.get("staff", pd. DataFrame())
        results = self.df. get("cert_results", pd. DataFrame())
        certs_overview = self.df. get("certificates", pd.DataFrame())
        todo = self.df. get("todo", pd.DataFrame())
        training_req = self.df. get("training_req", pd.DataFrame())
        
        if cfg.empty or staff.empty:
            print("   ⚠️ Geen config of staff data - niets te doen")
            return

        # 2. STRIKTE AFDELINGSFILTER (De 'Miserie' oplosser)
        id_col = self.get_id_column() or "staffGID"
        my_department_gids = set(staff[id_col].astype(str).str.strip().unique())
        
        cfg_col = next((c for c in [id_col, "staffGID"] if c in cfg. columns), None)
        def is_true(x): return str(x).lower() in ['true', '1', 'ja', 'yes', 't']
        
        # Filter config op alleen de mensen van JOUW afdeling die het certificaat NODIG hebben
        cfg_nodig = cfg[
            (cfg[cfg_col].astype(str).str.strip().isin(my_department_gids)) &
            (cfg["Nodig"]. apply(is_true))
        ].copy()
        
        print(f"   📊 Config voor {self.active_costcenter}:  {len(cfg_nodig)} rijen.")

        # 3. LOOKUPS BOUWEN (Alleen voor jouw mensen)
        # V19-FIX:  CostCenter zoeken in meerdere mogelijke kolomnamen
        # Na load_all() is 'staffCOSTCENTER315' hernoemd naar 'CostCenter'
        staff_lookup = {}
        for _, row in staff.iterrows():
            sid = str(row. get(id_col, "")).strip()
            
            # FIX: Zoek CostCenter in beide mogelijke kolomnamen + fallback naar active_costcenter
            cc_value = (
                row.get("CostCenter", "") or
                row. get("staffCOSTCENTER315", "") or
                self.active_costcenter or
                ""
            )
            
            staff_lookup[sid] = {
                "name": str(row.get("MedewerkerNaam", "") or row.get("FullName", "")).strip(),
                "sapnr": str(row.get("staffSAPNR", "")).strip(),
                "costcenter":  str(cc_value).strip()
            }

        # --- SMART LOOKUP (Best Info) ---
        best_info = {}
        def add_info(sid, cname, date_behaald, date_valid, date_issued, status, source):
            sid = str(sid).strip()
            if not sid or not cname or sid not in my_department_gids:
                return
            key = (sid, self.normalize_certname(cname))
            
            dt = pd.to_datetime(date_behaald, errors='coerce')
            valid = pd.to_datetime(date_valid, errors='coerce')
            issued = pd.to_datetime(date_issued, errors='coerce')
            
            is_better = False
            if key not in best_info: 
                is_better = True
            else:
                curr = best_info[key]
                if pd.notna(valid):
                    if pd.isna(curr["valid"]) or valid > curr["valid"]: 
                        is_better = True
                elif pd.notna(dt):
                    if pd.isna(curr["date"]) or dt > curr["date"]: 
                        is_better = True
            
            if is_better:
                best_info[key] = {"date": dt, "valid": valid, "issued": issued, "status": status, "source": source}

        # Vul best_info uit Results en Excel
        if not results.empty:
            r_id = next((c for c in ["staffGID", "MedewerkerID"] if c in results.columns), "staffGID")
            r_cert = next((c for c in ["CertName", "Certificaat"] if c in results.columns), "CertName")
            r_date = next((c for c in ["Behaald", "Behaald_Datum", "Exam_Date"] if c in results.columns), None)
            r_valid = next((c for c in ["Geldig_Tot", "ExpiryDate", "ValidUntil"] if c in results.columns), None)
            r_stat = next((c for c in ["Status", "Resultaat"] if c in results.columns), "Status")
            for _, row in results.iterrows():
                add_info(row. get(r_id), row.get(r_cert), row.get(r_date), row.get(r_valid), None, str(row.get(r_stat, "")), "results")

        if not certs_overview.empty:
            c_id = next((c for c in ["staffGID", "MedewerkerID"] if c in certs_overview.columns), "staffGID")
            c_cert = next((c for c in ["CertName", "Certificaat"] if c in certs_overview.columns), "CertName")
            c_valid = next((c for c in ["ExpiryDate", "Geldig_Tot"] if c in certs_overview.columns), None)
            c_issued = next((c for c in ["IssueDate", "Behaald"] if c in certs_overview.columns), None)
            for _, row in certs_overview.iterrows():
                add_info(row.get(c_id), row.get(c_cert), row.get(c_issued), row.get(c_valid), row.get(c_issued), "Certified", "certificates")

        # Inschrijvingen lookup
        inschrijving_lookup = {}
        if not training_req.empty:
            tr_id = "staffGID" if "staffGID" in training_req.columns else id_col
            tr_cert = "CertName" if "CertName" in training_req.columns else "Certificaat"
            for _, row in training_req.iterrows():
                sid = str(row. get(tr_id, "")).strip()
                if sid in my_department_gids:
                    knorm = self.normalize_certname(str(row.get(tr_cert, "")))
                    inschrijving_lookup[(sid, knorm)] = {
                        "date": pd. to_datetime(row.get("ScheduledDate", pd.NaT)),
                        "loc": str(row. get("Location", "")).strip()
                    }

        # 4. GENEREREN (Volledige V17-DEBUG logica)
        new_tasks = []
        now = pd.Timestamp.now()
        today = pd.Timestamp.today().normalize()
        debug_printed = 0
        existing_tasks = set(
            (str(r.get("staffGID", "")).strip(), str(r.get("CertName_norm", "")).strip())
            for _, r in todo.iterrows()
            if str(r.get("Status")).lower() not in ["afgewerkt", "gesloten"]
        )

        for _, row in cfg_nodig.iterrows():
            sid = str(row.get(cfg_col, "")).strip()
            cert_raw = str(row.get("CertName", "")).strip()
            cert_norm = self.normalize_certname(cert_raw)
            
            if sid not in my_department_gids or (sid, cert_norm) in existing_tasks: 
                continue
            s_info = staff_lookup. get(sid)
            
            # V19-FIX: Skip als we geen staff info hebben voor deze medewerker
            if not s_info:
                print(f"   ⚠️ Geen staff info voor {sid} - overgeslagen")
                continue

            # A. Check Inschrijving
            if (sid, cert_norm) in inschrijving_lookup:
                ins = inschrijving_lookup[(sid, cert_norm)]
                new_tasks.append({
                    "staffGID":  sid, "staffSAPNR": s_info["sapnr"], "MedewerkerID":  sid,
                    "MedewerkerNaam": s_info["name"], "CostCenter": s_info["costcenter"],
                    "CertName": cert_raw, "CertName_norm":  cert_norm, "TaskType": "Certificaat",
                    "Status": "Ingeschreven", "Status_Detail": f"Ingepland op {ins['date'].strftime('%d-%m-%Y') if pd.notna(ins['date']) else '?'}",
                    "Nodig": True, "Ingeschreven_Datum": ins["date"], "Ingeschreven_Locatie": ins["loc"],
                    "CreatedAt": now, "LastUpdatedAt": now, "CreatedBy": "sync_cert_tasks"
                })
                continue

            # B.  Bereken datums (De V17 functionaliteit)
            info = best_info.get((sid, cert_norm))
            status, detail, expiry, days_until, geldigheid = "Open", "Nog niet behaald", None, None, to_int_or_none(row.get("Interval_maanden"))

            if info: 
                if str(info["status"]).lower() in ["geslaagd", "passed", "certified", "ok", "behaald"]: 
                    valid, issued = info["valid"], info["issued"]
                    # 2099 / Oneindig fix
                    if pd.notna(valid) and valid.year >= 2099:
                        geldigheid = 0
                    elif geldigheid is None and pd. notna(valid) and pd.notna(issued):
                        diff = (valid - issued).days
                        if diff > 300:
                            geldigheid = int(round(diff / 30.44))

                    if pd.isna(valid) and pd.notna(info["date"]) and geldigheid: 
                        valid = info["date"] + pd.DateOffset(months=geldigheid)
                    
                    if pd.notna(valid):
                        days_until = int((valid - today).days)
                        if days_until > 180:
                            continue  # Nog geldig
                        detail = f"VERLOPEN - {abs(days_until)} dagen geleden" if days_until <= 0 else f"Verloopt binnenkort ({days_until} dagen)"
                        expiry = valid
                else:
                    detail = "Niet geslaagd - Herkansing nodig"

            new_tasks.append({
                "staffGID": sid, "staffSAPNR": s_info["sapnr"], "MedewerkerID": sid,
                "MedewerkerNaam":  s_info["name"], "CostCenter": s_info["costcenter"],
                "CertName": cert_raw, "CertName_norm": cert_norm, "TaskType":  "Certificaat",
                "Status":  status, "Status_Detail": detail, "Nodig": True,
                "Geldigheid_maanden": geldigheid, "ExpiryDate": expiry, "DaysUntilExpiry": days_until,
                "CreatedAt":  now, "LastUpdatedAt": now, "CreatedBy":  "sync_cert_tasks"
            })

        # 5. OPSLAAN - V20-FIX:  FutureWarning pd.concat opgelost
        if new_tasks:
            new_df = pd.DataFrame(new_tasks)
            # Verwijder lege kolommen om FutureWarning te voorkomen
            new_df = new_df.dropna(axis=1, how='all')
            if not todo.empty:
                todo = todo.dropna(axis=1, how='all')
            self.df["todo"] = pd.concat([todo, new_df], ignore_index=True)
            print(f"   ✅ {len(new_tasks)} nieuwe taken voor {self.active_costcenter}.")
            if self.USE_SQL_FOR_TODO: 
                self.save_todo_planner()
    
    def load_all(self, costcenter_filter: str = None) -> bool:
        """
        Laadt alle data gefilterd op het geselecteerde costcenter.
        - SQL-first voor staff/config/master/todo
        - Excel-imports voor certificates, cert_results, competences, training_req
        - Veilige checks (geen DataFrame direct in boolean context)
        - TaskID wordt NIET hernummerd; ontbrekende TaskID => pd.NA
        - STAP 15: subset-save (alleen recent gewijzigde rijen)
        """
        import pandas as pd
        from datetime import datetime, timedelta
        from pathlib import Path
        import time
        import traceback

        self.errors = []
        self.active_costcenter = costcenter_filter

        print("\n" + "=" * 60)
        print(f"📊 DATA LADEN - Costcenter: {costcenter_filter or 'ALLE'}")
        print("=" * 60)
        
        # 👇 VOEG DIT TOE: Laad eerst de vertalingen (HS -> Hoogspanning)
        # Dit zorgt ervoor dat alle data die hierna komt direct goed vertaald wordt.
        self.load_translations()
        
        # Helper: veilige SQL-call wrapper -> altijd DataFrame terug
        def _safe_sql_call(fn_name: str):
            if not getattr(self, "sql_training_manager", None):
                return pd.DataFrame()
            fn = getattr(self.sql_training_manager, fn_name, None)
            if not callable(fn):
                return pd.DataFrame()
            try:
                res = fn()
            except Exception as e:
                print(f"   ⚠️ Fout bij SQL-call {fn_name}: {e}")
                self.errors.append(f"Fout bij SQL-call {fn_name}: {e}")
                return pd.DataFrame()
            if res is None:
                return pd.DataFrame()
            if isinstance(res, pd.DataFrame):
                return res
            try:
                return pd.DataFrame(res)
            except Exception:
                return pd.DataFrame()


  
        # ========== STAP 1: STAFF LADEN (VOLLEDIGE LIJST) ==========
        try:
            print("\n📊 STAP 1: Staff laden...")
            
            # We halen ALTIJD alle medewerkers op. 
            # Dit is cruciaal om later taken van 'vreemde' medewerkers te kunnen identificeren en wegfilteren.
            if self.USE_SQL_FOR_STAFF and getattr(self, "sql_staff_manager", None):
                print("   → Bron: SQL Server (ophalen alle data voor lookup)")
                staff = self.sql_staff_manager.get_all_staff()
            else:
                print("   ❌ SQL niet beschikbaar voor staff")
                self.df["staff"] = pd.DataFrame()
                return False

            # Normalize result
            if staff is None or staff.empty:
                print("   ❌ SQL gaf geen staff data.")
                self.df["staff"] = pd.DataFrame()
                return False
            
            if not isinstance(staff, pd.DataFrame):
                staff = pd.DataFrame(staff)

            # 🔥 FIX 1: Hernoem de SQL kolom naar de interne naam 'CostCenter'
            # Dit moet gebeuren voordat we iets anders doen.
            if "staffCOSTCENTER315" in staff.columns:
                staff.rename(columns={"staffCOSTCENTER315": "CostCenter"}, inplace=True)
                print("   ✅ Kolom 'staffCOSTCENTER315' hernoemd naar 'CostCenter'.")
            elif "staffCOSTCENTER" in staff.columns and "CostCenter" not in staff.columns:
                staff.rename(columns={"staffCOSTCENTER": "CostCenter"}, inplace=True)

            # 🔥 FIX 2: GEEN CostCenter FILTER HIER!
            # We bewaren ALLE medewerkers in het geheugen. 
            # Dit stelt ons in staat om later te zien: "Hey, taak X hoort bij medewerker Y van afdeling W2".
            
            # Wel filteren op 'Actief' (mensen uit dienst hoeven we meestal niet meer)
            if "staffSTAFFSTATUSID" in staff.columns:
                staff["staffSTAFFSTATUSID"] = pd.to_numeric(staff["staffSTAFFSTATUSID"], errors="coerce")
                staff = staff[staff["staffSTAFFSTATUSID"] == 1].copy()

            # FullName aanmaken (voor de UI)
            if "staffFIRSTNAME" in staff.columns and "staffLASTNAME" in staff.columns:
                staff["FullName"] = (
                    staff["staffLASTNAME"].astype(str).str.strip() + ", " +
                    staff["staffFIRSTNAME"].astype(str).str.strip()
                )
            
            # IDs formatteren (belangrijk voor joins)
            if "staffGID" in staff.columns:
                staff["staffGID"] = staff["staffGID"].astype(str).str.strip()
            
            if "staffSAPNR" in staff.columns:
                staff["staffSAPNR"] = staff["staffSAPNR"].apply(self._normalize_sapnr)

            # Opslaan in geheugen
            self.df["staff"] = staff
            
            # Active staff IDs bevat nu IEDEREEN. 
            # Dit gebruiken we later misschien om te filteren, maar voor nu is het de complete set.
            active_staff_ids = set(staff["staffGID"].unique()) if "staffGID" in staff.columns else set()

            print(f"   ✅ STAFF: {len(staff)} actieve medewerkers geladen (Alle afdelingen)")

        except Exception as e:
            print(f"   ❌ STAFF fout: {e}")
            traceback.print_exc()
            self.errors.append(f"Fout bij laden STAFF: {e}")
            self.df["staff"] = pd.DataFrame()
            return False
        # ========== STAP 2: CERTIFICATES (Excel) - veilige variant ==========
        try:
            print("\n📜 STAP 2: Certificates laden (Excel)...")
            certs = pd.DataFrame()
            path = globals().get("INPUT_FILES", {}).get("certificates")
            if path:
                try:
                    p = Path(path)
                    if p.exists():
                        certs = pd.read_excel(p)
                    else:
                        print(f"   ⚠️ Certificates bestand niet gevonden: {p}")
                        self.errors.append("Certificates Excel niet gevonden")
                except Exception as e:
                    print(f"   ⚠️ Fout bij lezen certificates Excel: {e}")
                    self.errors.append(f"Fout bij lezen certificates Excel: {e}")
                    certs = pd.DataFrame()
            else:
                print("   ⚠️ Geen pad gedefinieerd voor certificates in INPUT_FILES")
                self.errors.append("Geen certificates input pad")

            if not certs.empty:
                # Bewaar originele CertName (defensief), zodat ensure_certname die niet onherroepelijk overschrijft
                if "CertName" in certs.columns:
                    try:
                        certs["__orig_CertName__"] = certs["CertName"].astype(str).fillna("")
                    except Exception:
                        certs["__orig_CertName__"] = certs["CertName"].astype(str).apply(lambda x: str(x) if x is not None else "")

                # Roep ensure_certname aan (kan de kolom CertName wijzigen) — we hebben backup nu
                try:
                    certs = ensure_certname(certs)
                except Exception:
                    # Als ensure_certname faalt, keep original certs (maar log)
                    import traceback as _tb; _tb.print_exc()

                # Als ensure_certname CertName heeft aangepast naar genormeerde waarde,
                # herstel dan de originele naam waar die beschikbaar was.
                try:
                    if "__orig_CertName__" in certs.columns:
                        def _restore_orig_certname(row):
                            orig = str(row.get("__orig_CertName__", "") or "").strip()
                            cur = row.get("CertName", "")
                            # Als origineel niet leeg/geen 'nan' gebruik origineel
                            if orig and orig.lower() != "nan":
                                return orig
                            # anders gebruik huidige waarde (zoals ensure_certname die heeft gezet)
                            return cur if cur is not None else ""
                        certs["CertName"] = certs.apply(_restore_orig_certname, axis=1)
                        # verwijder backup-kolom
                        certs.drop(columns=["__orig_CertName__"], inplace=True, errors="ignore")
                except Exception:
                    pass

                # Zorg dat we altijd een genormaliseerde kolom hebben (recompute)
                try:
                    if "CertName" in certs.columns:
                        certs["CertName"] = certs["CertName"].astype(str).str.strip()
                        certs["CertName_norm"] = certs["CertName"].apply(self.normalize_certname)
                    else:
                        certs["CertName_norm"] = ""
                except Exception:
                    certs["CertName_norm"] = certs.get("CertName_norm", "")

                # Expiry verwerking
                exp_col = None
                for c in ["ExpiryDate", "Expiry_Date", "Expiry Date", "Geldig_tot", "Valid_Until"]:
                    if c in certs.columns:
                        exp_col = c
                        break
                if exp_col:
                    certs["Expiry_Date"] = pd.to_datetime(certs[exp_col], errors="coerce")
                    try:
                        certs["Status"] = certs["Expiry_Date"].apply(status_from_expiry)
                    except Exception:
                        pass

                # Unique latest per staff+cert (bewaar laatste Expiry per staff+CertName)
                id_col_certs = None
                for c in ("staffGID", "staffSAPNR"):
                    if c in certs.columns:
                        id_col_certs = c
                        break
                if id_col_certs is not None and "CertName" in certs.columns and "Expiry_Date" in certs.columns:
                    try:
                        certs[id_col_certs] = certs[id_col_certs].astype(str).str.strip()
                        certs["CertName"] = certs["CertName"].astype(str).str.strip()
                        certs = certs.sort_values([id_col_certs, "CertName", "Expiry_Date"], ascending=[True, True, False], kind="mergesort")
                        certs = certs.drop_duplicates(subset=[id_col_certs, "CertName"], keep="first")
                    except Exception:
                        pass

                # Zorg dat CertName_norm aanwezig is en log indien nodig
                if "CertName" in certs.columns:
                    try:
                        certs["CertName_norm"] = certs["CertName"].apply(self.normalize_certname)
                        print("   ✅ CertName_norm toegevoegd aan certificates")
                    except Exception:
                        certs["CertName_norm"] = certs["CertName"].astype(str)

            self.df["certificates"] = certs
            print(f"   ✅ CERTIFICATES: {len(certs)} rijen")

        except Exception as e:
            print(f"   ❌ CERTIFICATES fout: {e}")
            traceback.print_exc()
            self.errors.append(f"Fout bij laden CERTIFICATES (Excel): {e}")
            self.df["certificates"] = pd.DataFrame()
        
        # ========== STAP 3: CERT RESULTS (Excel) ==========
        try:
            print("\n📋 STAP 3: Cert Results laden (Excel)...")
            cert_results = pd.DataFrame()
            path = globals().get("INPUT_FILES", {}).get("cert_results")
            if path:
                try:
                    p = Path(path)
                    if p.exists():
                        cert_results = pd.read_excel(p)
                    else:
                        print(f"   ⚠️ Cert results bestand niet gevonden: {p}")
                        self.errors.append("Cert results Excel niet gevonden")
                except Exception as e:
                    print(f"   ⚠️ Fout bij lezen cert_results Excel: {e}")
                    self.errors.append(f"Fout bij lezen cert_results Excel: {e}")
                    cert_results = pd.DataFrame()
            else:
                print("   ⚠️ Geen pad gedefinieerd voor cert_results in INPUT_FILES")
                self.errors.append("Geen cert_results input pad")

            if not cert_results.empty:
                try:
                    cert_results = ensure_certname(cert_results)
                except Exception:
                    pass
                for old_col, new_col in [("ExamDate", "Exam_Date"), ("Behaald", "Exam_Date"), ("Einde_sessie", "Exam_Date")]:
                    if old_col in cert_results.columns and "Exam_Date" not in cert_results.columns:
                        cert_results.rename(columns={old_col: "Exam_Date"}, inplace=True)
                        break
                if "Exam_Date" in cert_results.columns:
                    cert_results["Exam_Date"] = pd.to_datetime(cert_results["Exam_Date"], errors="coerce")
                for col in ["staffGID", "staffSAPNR"]:
                    if col in cert_results.columns:
                        cert_results[col] = cert_results[col].astype(str).str.strip()

            self.df["cert_results"] = cert_results
            print(f"   ✅ CERT_RESULTS: {len(cert_results)} rijen")

        except Exception as e:
            print(f"   ❌ CERT_RESULTS fout: {e}")
            traceback.print_exc()
            self.errors.append(f"Fout bij laden CERT_RESULTS (Excel): {e}")
            self.df["cert_results"] = pd.DataFrame()

               
        # ══════════════════════════════════════════════════════════════
        # STAP 4: TRAINING REQ (Excel import - geplande trainingen vanuit Xaurum)
        # ══════════════════════════════════════════════════════════════
        try:
            print("\n📅 STAP 4: Training Req laden (Excel)...")
            training_req = pd.DataFrame()
            path = INPUT_FILES.get("training_req")
            if path:
                try:
                    p = Path(path)
                    if p.exists():
                        training_req = pd.read_excel(p)
                    else:
                        print(f"   ⚠️ Training Req bestand niet gevonden: {p}")
                        self.errors.append("Training Req Excel niet gevonden")
                except Exception as e:
                    print(f"   ⚠️ Fout bij lezen training_req Excel: {e}")
                    self.errors.append(f"Fout bij lezen training_req Excel: {e}")
                    training_req = pd.DataFrame()
            else:
                print("   ⚠️ Geen pad gedefinieerd voor training_req in INPUT_FILES")
                self.errors.append("Geen training_req input pad")

            if not training_req.empty:
                training_req = ensure_certname(training_req)
                for col in ["staffGID", "staffSAPNR"]:
                    if col in training_req.columns:
                        training_req[col] = training_req[col].astype(str).str.strip()

                if "staffGID" in training_req.columns and active_staff_ids:
                    before = len(training_req)
                    training_req = training_req[training_req["staffGID"].isin(active_staff_ids)].copy()
                    removed = before - len(training_req)
                    if removed > 0:
                        print(f"   → Gefilterd: {len(training_req)} rijen (was {before}, -{removed})")

                rename_map = {}
                if "PlannedDate" in training_req.columns and "Planned_Date" not in training_req.columns:
                    rename_map["PlannedDate"] = "Planned_Date"
                if "RequestDate" in training_req.columns and "Request_Date" not in training_req.columns:
                    rename_map["RequestDate"] = "Request_Date"
                if rename_map:
                    training_req.rename(columns=rename_map, inplace=True)

                for c in ["Planned_Date", "Request_Date"]:
                    if c in training_req.columns:
                        training_req[c] = pd.to_datetime(training_req[c], errors="coerce")

                if "ScheduledDate" in training_req.columns:
                    training_req["ScheduledDateParsed"] = pd.to_datetime(training_req["ScheduledDate"], errors="coerce")

            self.df["training_req"] = training_req
            print(f"   ✅ TRAINING_REQ: {len(training_req)} rijen")

        except Exception as e:
            self.errors.append(f"Fout bij laden TRAINING_REQ (Excel): {e}")
            print(f"   ❌ TRAINING_REQ fout: {e}")
            self.df["training_req"] = pd.DataFrame()

        # ══════════════════════════════════════════════════════════════
        # STAP 5: COMPETENCES (Excel import)
        # ══════════════════════════════════════════════════════
        try:
            print("\n🎯 STAP 5: Competences laden (Excel)...")
            df_comp = pd.DataFrame()
            path = INPUT_FILES.get("competences")
            if path:
                try:
                    p = Path(path)
                    if p.exists():
                        df_comp = pd.read_excel(p)
                    else:
                        print(f"   ⚠️ Competences bestand niet gevonden: {p}")
                        self.errors.append("Competences Excel niet gevonden")
                except Exception as e:
                    print(f"   ⚠️ Fout bij lezen competences Excel: {e}")
                    self.errors.append(f"Fout bij lezen competences Excel: {e}")
                    df_comp = pd.DataFrame()
            else:
                print("   ⚠️ Geen pad gedefinieerd voor competences in INPUT_FILES")
                self.errors.append("Geen competences input pad")

            if not df_comp.empty:
                id_col = self.get_id_column() or "staffGID"
                if id_col in df_comp.columns and active_staff_ids:
                    df_comp[id_col] = df_comp[id_col].astype(str).str.strip()
                    before = len(df_comp)
                    df_comp = df_comp[df_comp[id_col].isin(active_staff_ids)].copy()
                    removed = before - len(df_comp)
                    if removed > 0:
                        print(f"   → Gefilterd: {len(df_comp)} rijen (was {before}, -{removed})")

                if "Competence" not in df_comp.columns:
                    for cand in ("CompName", "Competentie"):
                        if cand in df_comp.columns:
                            df_comp = df_comp.rename(columns={cand: "Competence"})
                            break

                if "Competence" in df_comp.columns:
                    df_comp["Competence"] = df_comp["Competence"].astype(str).str.strip()
                    df_comp["Competence_norm"] = df_comp["Competence"].apply(self.normalize_certname)

            self.df["competences"] = df_comp
            print(f"   ✅ COMPETENCES: {len(df_comp)} rijen")

        except Exception as e:
            self.errors.append(f"Fout bij laden COMPETENCES (Excel): {e}")
            print(f"   ❌ COMPETENCES fout: {e}")
            self.df["competences"] = pd.DataFrame()

        # ══════════════════════════════════════════════════════════════
        # STAP 6 & 7: MASTER CERTIFICATEN en MASTER COMPETENTIES (SQL)
        # ══════════════════════════════════════════════════════════════
        try:
            print("\n📚 STAP 6: Master Certificaten laden...")
            master_cert = _safe_sql_call("get_master_certificaten")
            if master_cert.empty:
                print("   ⚠️ SQL: Geen master certificaten gevonden")
                self.master_cert_all = pd.DataFrame(columns=["CertName"])
                self.master_cert_req = pd.DataFrame(columns=["CertName"])
            else:
                print(f"   ✅ SQL: {len(master_cert)} master certificaten")
                if "CertName" not in master_cert.columns:
                    for col in ["Competency_Name", "Naam", "Name", "Opleiding"]:
                        if col in master_cert.columns:
                            master_cert["CertName"] = master_cert[col].astype(str).str.strip()
                            break
                strat_col = None
                for col in ["StrategischBelangrijk", "Strategisch", "IsStrategic"]:
                    if col in master_cert.columns:
                        strat_col = col
                        break
                active_col = None
                for col in ["Active", "Actief", "Enabled"]:
                    if col in master_cert.columns:
                        active_col = col
                        break
                master_cert["__is_strat__"] = master_cert[strat_col].apply(is_truthy_value) if strat_col else False
                master_cert["__is_active__"] = master_cert[active_col].apply(is_truthy_value) if active_col else True
                self.master_cert_req = (
                    master_cert[(master_cert["__is_strat__"]) & (master_cert["__is_active__"])][["CertName"]]
                    .drop_duplicates()
                )
                self.master_cert_all = master_cert[["CertName"]].drop_duplicates().sort_values("CertName")
                self.df["master_cert"] = master_cert
                print(f"   ✅ Master Cert: {len(self.master_cert_all)} totaal, {len(self.master_cert_req)} strategisch")
        except Exception as e:
            self.errors.append(f"Fout in Master_Certificaten: {e}")
            print(f"   ❌ Master Cert fout: {e}")
            import traceback; traceback.print_exc()
            self.master_cert_req = pd.DataFrame(columns=["CertName"])
            self.master_cert_all = pd.DataFrame(columns=["CertName"])

        try:
            print("\n🎯 STAP 7: Master Competenties laden...")
            master_comp = _safe_sql_call("get_master_competenties")
            if master_comp.empty:
                print("   ⚠️ SQL: Geen master competenties gevonden")
                self.master_comp_all = pd.DataFrame(columns=["Competence"])
                self.master_comp_req = pd.DataFrame(columns=["Competence"])
            else:
                print(f"   ✅ SQL: {len(master_comp)} master competenties")
                if "Competence" not in master_comp.columns:
                    for col in ["Competency_Name", "Competency", "Naam", "Name"]:
                        if col in master_comp.columns:
                            master_comp["Competence"] = master_comp[col].astype(str).str.strip()
                            break
                strat_col = None
                for col in ["StrategischBelangrijk", "Strategisch", "IsStrategic"]:
                    if col in master_comp.columns:
                        strat_col = col
                        break
                act_col = None
                for col in ["Active", "Actief", "Enabled"]:
                    if col in master_comp.columns:
                        act_col = col
                        break
                master_comp["__is_strat__"] = master_comp[strat_col].apply(is_truthy_value) if strat_col else False
                master_comp["__is_active__"] = master_comp[act_col].apply(is_truthy_value) if act_col else True
                self.master_comp_req = (
                    master_comp[(master_comp["__is_strat__"]) & (master_comp["__is_active__"])][["Competence"]]
                    .drop_duplicates()
                )
                self.master_comp_all = master_comp[["Competence"]].drop_duplicates().sort_values("Competence")
                print(f"   ✅ Master Comp: {len(self.master_comp_all)} totaal, {len(self.master_comp_req)} strategisch")
        except Exception as e:
            self.errors.append(f"Fout in Master_Competenties: {e}")
            print(f"   ❌ Master Comp fout: {e}")
            import traceback; traceback.print_exc()
            self.master_comp_req = pd.DataFrame(columns=["Competence"])
            self.master_comp_all = pd.DataFrame(columns=["Competence"])

        # ══════════════════════════════════════════════════════════════
        # STAP 8: CONFIG CERTIFICATEN (SQL)
        # ══════════════════════════════════════════════════════════════
        try:
            print("\n⚙️ STAP 8: Config Certificaten laden...")
            cfg = _safe_sql_call("get_medewerker_certificaat_config")

            if cfg.empty:
                cfg = pd.DataFrame(columns=[
                    "ConfigID", "staffGID", "staffSAPNR", "FullName",
                    "CertName", "CertName_norm", "Nodig", "Strategisch",
                    "Interval_maanden", "Opmerking", "LaatsteWijziging", "GewijzigdDoor"
                ])
            else:
                for col in ["staffGID", "staffSAPNR"]:
                    if col in cfg.columns:
                        cfg[col] = cfg[col].astype(str).str.strip()
                if "staffGID" in cfg.columns and active_staff_ids:
                    before = len(cfg)
                    cfg = cfg[cfg["staffGID"].isin(active_staff_ids)].copy()
                    removed = before - len(cfg)
                    if removed > 0:
                        print(f"   → Gefilterd: {len(cfg)} rijen (was {before}, -{removed})")
                if "CertName" in cfg.columns:
                    cfg["CertName"] = cfg["CertName"].astype(str).str.strip()
                for col in ["Nodig", "Strategisch"]:
                    if col in cfg.columns:
                        cfg[col] = cfg[col].apply(is_truthy_value)

            self.df["config_cert"] = cfg
            self.df["config"] = cfg
            print(f"   ✅ Config Cert: {len(cfg)} rijen")

        except Exception as e:
            self.errors.append(f"Fout bij laden CONFIG_CERT: {e}")
            print(f"   ❌ Config Cert fout: {e}")
            import traceback; traceback.print_exc()
            self.df["config_cert"] = pd.DataFrame()
            self.df["config"] = pd.DataFrame()
        
        # ══════════════════════════════════════════════════════════════
        # STAP 9: CONFIG COMPETENTIES (SQL)
        # ══════════════════════════════════════════════════════════════
        try:
            print("\n🎯 STAP 9: Config Competenties laden...")
            df_cfg = _safe_sql_call("get_medewerker_competentie_config")

            if df_cfg.empty:
                df_cfg = pd.DataFrame(columns=[
                    "ConfigID", "staffGID", "staffSAPNR", "FullName",
                    "Competence", "Competence_norm", "Nodig",
                    "Interval_maanden", "Opmerking", "LaatsteWijziging", "GewijzigdDoor"
                ])
            else:
                id_col = self.get_id_column() or "staffGID"
                if id_col in df_cfg.columns:
                    df_cfg[id_col] = df_cfg[id_col].astype(str).str.strip()
                    if active_staff_ids:
                        before = len(df_cfg)
                        df_cfg = df_cfg[df_cfg[id_col].isin(active_staff_ids)].copy()
                        removed = before - len(df_cfg)
                        if removed > 0:
                            print(f"   → Gefilterd: {len(df_cfg)} rijen (was {before}, -{removed})")
                if "Competence" not in df_cfg.columns:
                    for cand in ("CompName", "Competentie"):
                        if cand in df_cfg.columns:
                            df_cfg = df_cfg.rename(columns={cand: "Competence"})
                            break
                if "Competence" in df_cfg.columns:
                    df_cfg["Competence"] = df_cfg["Competence"].astype(str).str.strip()
                    df_cfg["Competence_norm"] = df_cfg["Competence"].apply(self.normalize_certname)
                if "Nodig" in df_cfg.columns:
                    df_cfg["Nodig"] = df_cfg["Nodig"].apply(is_truthy_value)
                else:
                    df_cfg["Nodig"] = False
                if "Interval_maanden" in df_cfg.columns:
                    df_cfg["Interval_maanden"] = df_cfg["Interval_maanden"].fillna(0).astype(int)
                else:
                    df_cfg["Interval_maanden"] = 0
                if "Opmerking" not in df_cfg.columns:
                    df_cfg["Opmerking"] = ""

            self.df["competence_config"] = df_cfg
            print(f"   ✅ Config Comp: {len(df_cfg)} rijen")

        except Exception as e:
            self.errors.append(f"Fout bij laden COMPETENCE_CONFIG: {e}")
            print(f"   ❌ Config Comp fout: {e}")
            import traceback; traceback.print_exc()
            self.df["competence_config"] = pd.DataFrame()

        # =========================================================
        # 🧼 STAP 9.5: AUTO-CLEANUP (De "Wasstraat")
        # =========================================================
        # Dit repareert Franse/Engelse namen in de SQL database
        # VOORDAT de Todo Planner (Stap 13) gaat rekenen.
        if self.USE_SQL_FOR_CONFIG:
            try:
                # 1. Voer de cleanup uit op SQL (Certificaten)
                self.clean_sql_config_names()
                
                # 2. Omdat we net SQL hebben aangepast, is de in-memory data 
                # van STAP 8 ("config_cert") mogelijk verouderd (nog in het Frans).
                # We herladen die specifieke tabel even snel opnieuw uit SQL.
                if self.sql_training_manager:
                    print("   🔄 Config Certificaten herladen na cleanup...")
                    # We gebruiken de interne methode of een nieuwe SQL call
                    df_clean = _safe_sql_call("get_medewerker_certificaat_config")
                    
                    # Pas dezelfde filters toe als in Stap 8
                    if not df_clean.empty and active_staff_ids:
                        id_col = self.get_id_column() or "staffGID"
                        if id_col in df_clean.columns:
                            df_clean[id_col] = df_clean[id_col].astype(str).str.strip()
                            df_clean = df_clean[df_clean[id_col].isin(active_staff_ids)].copy()
                    
                    self.df["config_cert"] = df_clean
                    
            except Exception as e:
                print(f"   ⚠️ Cleanup warning: {e}")

        # =========================================================
        # STAP 10: TODO PLANNER LADEN (MET SQL OOGKLEPPEN)
        # =========================================================
        try:
            print("\n📋 STAP 10: Todo Planner laden uit SQL...")
            
            # 1. Bepaal de filter (De Oogkleppen)
            active_filter = None
            if self.active_costcenter:
                active_filter = str(self.active_costcenter).strip()

            # 2. Haal data op MET filter direct in SQL
            if self.sql_training_manager:
                # 🔥 HIER GEBRUIKEN WE DE NIEUWE FILTER-FUNCTIE
                # Als active_filter None is (bijv. bij opstarten zonder keuze), haalt hij alles (of niks, afhankelijk van je manager logica).
                # Maar zodra er een keuze is gemaakt, krijgt SQL de opdracht: WHERE CostCenter = '...'
                todo_sql = self.sql_training_manager.get_todo_planner(costcenter=active_filter)
            else:
                todo_sql = pd.DataFrame()

            # 3. Verwerking (Lege DF als niks gevonden)
            if todo_sql is None or todo_sql.empty:
                print(f"   ℹ️ SQL: Geen taken gevonden voor {active_filter or 'alles'}")
                cols = [
                    "TaskID", "staffGID", "staffSAPNR", "MedewerkerID",
                    "MedewerkerNaam", "CostCenter", "CertName", "CertName_norm",
                    "TaskType", "Status", "Status_Detail", "Nodig", "Commentaar",
                    "Ingeschreven_Datum", "Ingeschreven_Locatie",
                    "CreatedAt", "LastUpdatedAt", "CreatedBy",
                    "Geldigheid_maanden", "ExpiryDate", "DaysUntilExpiry"
                ]
                todo_sql = pd.DataFrame(columns=cols)
            
            else:
                # Als er wel data is:
                print(f"   ✅ SQL: {len(todo_sql)} taken geladen (exclusief voor {active_filter})")
                
                # 🛡️ 4. EXTRA VEILIGHEID (Python Filter)
                # Voor het geval er ooit iets geks in de DB staat met spaties of als de SQL-manager de filter negeerde,
                # filteren we nog 1x keihard na in het geheugen.
                if active_filter and "CostCenter" in todo_sql.columns:
                    todo_sql["CostCenter"] = todo_sql["CostCenter"].astype(str).str.strip()
                    
                    initial_count = len(todo_sql)
                    # Filter strikt op het actieve costcenter
                    todo_sql = todo_sql[todo_sql["CostCenter"] == active_filter].copy()
                    
                    filtered_count = len(todo_sql)
                    if initial_count != filtered_count:
                        print(f"   🛡️ MEMORY FILTER: {initial_count - filtered_count} taken van andere afdelingen verborgen.")

                # 🛡️ 5. SCHEMA CLEANUP
                if "Competence" in todo_sql.columns:
                    todo_sql = todo_sql.drop(columns=["Competence"])
                
                # Types herstellen
                date_cols = ["Ingeschreven_Datum", "ExpiryDate", "CreatedAt", "LastUpdatedAt"]
                for col in date_cols:
                    if col in todo_sql.columns:
                        todo_sql[col] = pd.to_datetime(todo_sql[col], errors='coerce')

                num_cols = ["Geldigheid_maanden", "DaysUntilExpiry", "TaskID"]
                for col in num_cols:
                    if col in todo_sql.columns:
                        todo_sql[col] = pd.to_numeric(todo_sql[col], errors='coerce')

            # 4. Opslaan in geheugen
            self.df["todo"] = todo_sql
            print(f"   ✅ TODO: {len(self.df['todo'])} taken in geheugen.")

        except Exception as e:
            print(f"   ❌ Fout bij laden TodoPlanner: {e}")
            import traceback; traceback.print_exc()
            self.df["todo"] = pd.DataFrame()
        # =========================================================
        # ⚡ STAP 10.5: VERRIJKING (Data Reparatie)
        # =========================================================
        # Omdat de SQL tabel soms kolommen mist (zoals CostCenter of SAPNR bij oude taken), 
        # vullen we ze hier aan vanuit de geladen STAFF lijst (die IEDEREEN bevat).
        
        todo = self.df.get("todo", pd.DataFrame())
        staff = self.df.get("staff", pd.DataFrame())

        if not todo.empty and not staff.empty:
            print("   🔧 Data Verrijking: Ontbrekende medewerker-info aanvullen...")
            
            # Zorg voor schone keys
            if "staffGID" in todo.columns and "staffGID" in staff.columns:
                # Maak mapping dictionaries voor snelle lookup
                staff["staffGID"] = staff["staffGID"].astype(str).str.strip()
                
                # Mapping: GID -> CostCenter
                # Let op: 'staff' bevat nu ALLE medewerkers (dankzij Stap 1 fix), dus we vinden ook de 'vreemde'
                cc_map = staff.set_index("staffGID")["CostCenter"].to_dict()
                
                # Mapping: GID -> SAPNR
                sap_map = {}
                if "staffSAPNR" in staff.columns:
                    sap_map = staff.set_index("staffGID")["staffSAPNR"].to_dict()
                
                # Functie om CostCenter te fixen als het leeg is
                def fix_cc(row):
                    curr = str(row.get("CostCenter", "")).strip()
                    if curr and curr.lower() != "nan" and curr.lower() != "none" and curr.lower() != "nat":
                        return curr
                    gid = str(row.get("staffGID", "")).strip()
                    return cc_map.get(gid, "")

                # Functie om SAPNR te fixen als het leeg is
                def fix_sap(row):
                    curr = str(row.get("staffSAPNR", "")).strip()
                    if curr and curr.lower() != "nan" and curr.lower() != "none":
                        return curr
                    gid = str(row.get("staffGID", "")).strip()
                    return sap_map.get(gid, "")

                # Pas toe
                todo["CostCenter"] = todo.apply(fix_cc, axis=1)
                todo["staffSAPNR"] = todo.apply(fix_sap, axis=1)
                
                # Vul ook MedewerkerID (is vaak gelijk aan GID)
                if "MedewerkerID" in todo.columns:
                    todo["MedewerkerID"] = todo["MedewerkerID"].fillna(todo["staffGID"])

                print(f"   ✅ Verrijking voltooid voor {len(todo)} taken.")
                self.df["todo"] = todo
        
        # =========================================================
        # 🧹 STAP 10.6: GEHEUGEN OPSCHONEN (STAFF FILTER)
        # =========================================================
        # Nu de verrijking klaar is, gooien we medewerkers van andere afdelingen uit het geheugen.
        # Dit voorkomt dat Stap 13 taken gaat genereren voor mensen die niet bij de actieve afdeling horen.
        
        if self.active_costcenter and "staff" in self.df:
            staff_full = self.df["staff"]
            target_cc = str(self.active_costcenter).strip()
            
            if "CostCenter" in staff_full.columns:
                # Filter de staff tabel nu pas
                staff_filtered = staff_full[staff_full["CostCenter"].astype(str).str.strip() == target_cc].copy()
                
                self.df["staff"] = staff_filtered
                
                # Update de set met actieve IDs (belangrijk voor Stap 13!)
                active_staff_ids = set(staff_filtered["staffGID"].astype(str).str.strip().unique())
                
                print(f"   🧹 STAFF FILTER: Teruggebracht naar {len(staff_filtered)} medewerkers van {target_cc}.")
            else:
                print("   ⚠️ Kan staff niet filteren: kolom 'CostCenter' ontbreekt.")
        # =========================================================
        # =========================================================
        # STAP 11: TRAINING CATALOG (SQL)
        try:
            print("\n📚 STAP 11: Training Catalog laden...")
            df_cat = _safe_sql_call("get_training_catalogus")
            if not df_cat.empty:
                if "Title" in df_cat.columns and "title" not in df_cat.columns:
                    df_cat["title"] = df_cat["Title"]
                if "Url" in df_cat.columns and "url" not in df_cat.columns:
                    df_cat["url"] = df_cat["Url"]
                if "Code" in df_cat.columns and "code" not in df_cat.columns:
                    df_cat["code"] = df_cat["Code"]
                if "raw_text" not in df_cat.columns and "title" in df_cat.columns:
                    df_cat["raw_text"] = df_cat["title"]
            self.training_catalog = df_cat
            print(f"   ✅ Training Catalog: {len(df_cat)} rijen" if not df_cat.empty else "   ℹ️ Training Catalog leeg")
        except Exception as e:
            print(f"   ❌ FOUT: {e}")
            self.errors.append(f"❌ Kan training catalog niet laden: {e}")
            self.training_catalog = pd.DataFrame()
        
        # STAP 12: HELPER SETS
        print("\n🔍 STAP 12: Helper sets bouwen...")
        self.all_cert_names = set()
        self.all_competence_names = set()
        try:
            if hasattr(self, "master_cert_all") and not self.master_cert_all. empty and "CertName" in self.master_cert_all. columns:
                self.all_cert_names = set(self.master_cert_all["CertName"].dropna().astype(str).unique())
            if hasattr(self, "master_comp_all") and not self.master_comp_all.empty and "Competence" in self.master_comp_all. columns:
                self.all_competence_names = set(self.master_comp_all["Competence"].dropna().astype(str).unique())
        except Exception as e: 
            print(f"   ⚠️ Fout bij bouwen zoeksets: {e}")
        print(f"   ✅ Zoeksets:  {len(self. all_cert_names)} certs, {len(self.all_competence_names)} comps")

        # =========================================================
        # STAP 13: SMART SYNC (Inschrijvingen & Failed Results)
        # =========================================================
        try:
            try:
                print("\n🔄 Sync inschrijvingen uit Training_Req...")
                inschrijvingen_count = self.sync_inschrijvingen()
                if inschrijvingen_count > 0:
                    print(f"   ✅ {inschrijvingen_count} taken bijgewerkt met inschrijfdata")
                else: 
                    print("   ℹ️ Geen nieuwe inschrijvingen gevonden")
            except Exception as e: 
                print(f"   ⚠️ Fout bij sync_inschrijvingen: {e}")
                import traceback; traceback.print_exc()

            try:
                print("\n🔄 Sync niet-geslaagde resultaten uit Cert_Results...")
                n_failed = self. sync_failed_results_to_todo()
                if n_failed > 0:
                    print(f"   ✅ {n_failed} niet-geslaagde taak/taken aangemaakt/heropend")
                else: 
                    print("   ℹ️ Geen niet-geslaagde taken nodig")
            except Exception as e: 
                print(f"   ⚠️ Fout bij sync_failed_results_to_todo: {e}")
                import traceback; traceback.print_exc()
        except Exception as e:
            print(f"\n⚠️ Fout bij smart sync check: {e}")

        # STAP 14: close_finished_tasks
        print("\n🔄 STAP 14: Status updates toepassen...")
        try:
            changes_count = self.close_finished_tasks()
            # Detecteer afwezigen bij afgelopen opleidingen
            self.detect_absent_from_completed_training()
            if changes_count and changes_count > 0:
                print(f"   → {changes_count} taken gewijzigd")
            else:
                print("   ✅ Geen wijzigingen nodig")
            print("   ✅ Status updates voltooid")
        except Exception as e: 
            print(f"   ⚠️ Fout bij close_finished_tasks: {e}")

        # STAP 14. 5: Opschonen taken voor medewerkers die uit dienst zijn
        print("\n🧹 STAP 14.5: Taken opschonen voor inactieve medewerkers...")
        try:
            inactief_count = self.close_tasks_for_inactive_staff()
            if inactief_count > 0:
                print(f"   → {inactief_count} taken afgesloten (medewerkers uit dienst)")
        except Exception as e: 
            print(f"   ⚠️ Fout bij close_tasks_for_inactive_staff: {e}")

        # STAP 15: Naam conversie en subset-save
        print("\n💾 STAP 15: Naam conversie en subset-save (met TaskID mapping)...")
        try:
            todo = self.df.get("todo", pd. DataFrame())
            needs_save = False
            modified_count = 0

            if not todo.empty:
                names_converted = self.convert_names_to_lastname_first()
                if names_converted is None:
                    names_converted = 0
                if names_converted > 0:
                    print(f"   → {names_converted} namen geconverteerd")
                    needs_save = True
                    modified_count += names_converted
                else:
                    print("   ✅ Alle namen zijn al in correct formaat")

                recently_modified = pd.DataFrame()
                try:
                    if "LastUpdatedAt" in todo.columns:
                        now_dt = datetime.now()
                        recent_threshold = now_dt - timedelta(seconds=10)
                        todo_check = todo. copy()
                        todo_check["LastUpdatedAt"] = pd.to_datetime(todo_check["LastUpdatedAt"], errors="coerce")
                        recently_modified = todo_check[
                            todo_check["LastUpdatedAt"]. notna() & (todo_check["LastUpdatedAt"] > recent_threshold)
                        ].copy()
                        recent_count = len(recently_modified)
                        if recent_count > 0:
                            print(f"   → {recent_count} taken gewijzigd in deze sessie")
                            needs_save = True
                            modified_count = max(modified_count, recent_count)
                except Exception as e:
                    print(f"   ⚠️ Kon LastUpdatedAt niet checken: {e}")

                if needs_save and modified_count > 0:
                    rows_to_save = recently_modified.copy()
                    expected_cols = [
                        "TaskID", "staffGID", "staffSAPNR", "MedewerkerID", "MedewerkerNaam",
                        "CostCenter", "CertName", "CertName_norm", "TaskType", "Status",
                        "Status_Detail", "Nodig", "Commentaar", "Ingeschreven_Datum",
                        "Ingeschreven_Locatie", "CreatedAt", "LastUpdatedAt", "CreatedBy",
                        "Geldigheid_maanden", "ExpiryDate", "DaysUntilExpiry"
                    ]
                    for c in expected_cols: 
                        if c not in rows_to_save.columns:
                            rows_to_save[c] = None

                    rows_to_save["TaskID"] = rows_to_save["TaskID"].apply(
                        lambda x:  None if (pd.isna(x) or str(x).strip().lower() == "nan" or str(x).strip() == "") else x
                    )

                    import uuid
                    rows_to_save["_SrcRowId"] = [str(uuid.uuid4()) for _ in range(len(rows_to_save))]

                    print(f"\n   💾 Opslaan subset:  {len(rows_to_save)} gewijzigde taken naar SQL...")
                    if self.USE_SQL_FOR_TODO and getattr(self, "sql_training_manager", None):
                        try:
                            success, mapping = self.sql_training_manager.save_todo_planner(rows_to_save)
                            if success:
                                print("   ✅ Subset todo opgeslagen naar SQL Server")
                            else:
                                print("   ⚠️ Subset save gaf False")
                                self.errors.append("TODO subset save faalde")
                        except Exception as e: 
                            print(f"   ❌ Exception tijdens save_todo_planner: {e}")
                            self.errors.append(f"Subset-save exception: {e}")
                    else: 
                        print("   ❌ Kan TODO niet opslaan (SQL niet beschikbaar)")
                else:
                    print("   ✅ Geen wijzigingen - SKIP SAVE")
            else:
                print("   ⚠️ Geen todo data om op te slaan")
        except Exception as e:
            print(f"   ⚠️ Fout tijdens STAP 15 opslaan:  {e}")
            self.errors.append(f"Fout tijdens opslaan: {e}")

        # STAP 16: check training_req tegen config (Stille controle)
        try:
            print("\n🔍 STAP 16: Controleren op inschrijvingen zonder config...")
            check_result = self. check_training_req_against_config()
            missing_count = check_result. get("missing_count", 0) if isinstance(check_result, dict) else 0
            
            if missing_count > 0:
                print(f"   ℹ️ INFO: {missing_count} inschrijving(en) gevonden die nog niet in config staan.")
                for item in check_result.get("missing_items", [])[:3]:
                    print(f"      • {item. get('medewerker')} → {item.get('cert_name')}")
            else:
                print("   ✅ Alle inschrijvingen correct gesynchroniseerd.")
        except Exception as e:
            print(f"   ⚠️ Debug:  Fout bij check training_req:  {e}")

        # =========================================================
        # 🔧 STAP 17: SYNC NIEUWE TAKEN (VEILIG - ALLEEN TOEVOEGEN)
        # =========================================================
        print("\n" + "=" * 60)
        print("🔄 STAP 17: Check voor nieuwe taken (veilige modus)...")
        print("=" * 60)
        
        try: 
            if hasattr(self, "sync_cert_tasks"):
                self.sync_cert_tasks()
            
            if hasattr(self, "sync_competence_tasks"):
                self. sync_competence_tasks()
            
            if hasattr(self, "enrich_todo_with_staff_info"):
                self.enrich_todo_with_staff_info()
            
            if hasattr(self, "save_todo"):
                self.save_todo()
                
            print("✅ STAP 17: Sync voltooid (bestaande taken intact)")
            
        except Exception as e: 
            print(f"⚠️ Fout bij STAP 17 sync: {e}")
            import traceback
            traceback.print_exc()

        # =========================================================
        # Return summary
        # =========================================================
        print("\n" + "=" * 60)
        print(f"✅ load_all() VOLTOOID - Costcenter: {costcenter_filter or 'ALLE'}")
        print("=" * 60)
        staff_count = len(self. df.get("staff", pd.DataFrame()))
        cert_count = len(self. df.get("certificates", pd.DataFrame()))
        todo_count = len(self. df.get("todo", pd.DataFrame()))
        print(f"   📊 Samenvatting:")
        print(f"      • Staff: {staff_count} medewerkers")
        print(f"      • Certificates: {cert_count} rijen")
        print(f"      • Todo:  {todo_count} taken")
        if self.errors:
            print(f"\n   ⚠️ {len(self.errors)} waarschuwing(en):")
            for err in self.errors[:20]:
                print(f"      - {err}")
        print("=" * 60 + "\n")
        
        return True
        # # STAP 12: HELPER SETS
        # print("\n🔍 STAP 12: Helper sets bouwen...")
        # self.all_cert_names = set()
        # self.all_competence_names = set()
        # try:
            # if hasattr(self, "master_cert_all") and not self.master_cert_all.empty and "CertName" in self.master_cert_all.columns:
                # self.all_cert_names = set(self.master_cert_all["CertName"].dropna().astype(str).unique())
            # if hasattr(self, "master_comp_all") and not self.master_comp_all.empty and "Competence" in self.master_comp_all.columns:
                # self.all_competence_names = set(self.master_comp_all["Competence"].dropna().astype(str).unique())
        # except Exception as e:
            # print(f"   ⚠️ Fout bij bouwen zoeksets: {e}")
        # print(f"   ✅ Zoeksets: {len(self.all_cert_names)} certs, {len(self.all_competence_names)} comps")

       # SKIP SYNCSKIP SYNCSKIP SYNCSKIP SYNC
            # try:
                # print("\n🔄 Sync inschrijvingen uit Training_Req...")
                # inschrijvingen_count = self.sync_inschrijvingen()
                # if inschrijvingen_count > 0:
                    # print(f"   ✅ {inschrijvingen_count} taken bijgewerkt met inschrijfdata")
                # else:
                    # print("   ℹ️ Geen nieuwe inschrijvingen gevonden")
            # except Exception as e:
                # print(f"   ⚠️ Fout bij sync_inschrijvingen: {e}")
                # import traceback; traceback.print_exc()

            # # STAP 13c: Niet-geslaagde resultaten -> zorg dat er taken bestaan (per costcenter)
            # try:
                # print("\n🔄 Sync niet-geslaagde resultaten uit Cert_Results...")
                # n_failed = self.sync_failed_results_to_todo()
                # if n_failed > 0:
                    # print(f"   ✅ {n_failed} niet-geslaagde taak/taken aangemaakt/heropend")
                # else:
                    # print("   ℹ️ Geen niet-geslaagde taken nodig")
            # except Exception as e:
                # print(f"   ⚠️ Fout bij sync_failed_results_to_todo: {e}")
                # import traceback; traceback.print_exc()
        # except Exception as e:
            # print(f"\n⚠️ Fout bij smart sync check: {e}")

        # # STAP 14: close_finished_tasks
        # print("\n🔄 STAP 14: Status updates toepassen...")
        # try:
            # changes_count = self.close_finished_tasks()
            # if changes_count and changes_count > 0:
                # print(f"   → {changes_count} taken gewijzigd")
            # else:
                # print("   ✅ Geen wijzigingen nodig")
            # print("   ✅ Status updates voltooid")
        # except Exception as e:
            # print(f"   ⚠️ Fout bij close_finished_tasks: {e}")
        # # STAP 14.5: Opschonen taken voor medewerkers die uit dienst zijn
        # print("\n🧹 STAP 14.5: Taken opschonen voor inactieve medewerkers...")
        # try:
            # inactief_count = self.close_tasks_for_inactive_staff()
            # if inactief_count > 0:
                # print(f"   → {inactief_count} taken afgesloten (medewerkers uit dienst)")
        # except Exception as e:
            # print(f"   ⚠️ Fout bij close_tasks_for_inactive_staff: {e}")
        
        # # STAP 15: Naam conversie en subset-save (alleen recent gewijzigde rijen)
        # print("\n💾 STAP 15: Naam conversie en subset-save (met TaskID mapping)...")
        # try:
            # todo = self.df.get("todo", pd.DataFrame())
            # needs_save = False
            # modified_count = 0

            # if not todo.empty:
                # names_converted = self.convert_names_to_lastname_first()
                # if names_converted is None:
                    # names_converted = 0
                # if names_converted > 0:
                    # print(f"   → {names_converted} namen geconverteerd")
                    # needs_save = True
                    # modified_count += names_converted
                # else:
                    # print("   ✅ Alle namen zijn al in correct formaat")

                # recently_modified = pd.DataFrame()
                # try:
                    # if "LastUpdatedAt" in todo.columns:
                        # now_dt = datetime.now()
                        # recent_threshold = now_dt - timedelta(seconds=10)
                        # todo_check = todo.copy()
                        # todo_check["LastUpdatedAt"] = pd.to_datetime(todo_check["LastUpdatedAt"], errors="coerce")
                        # recently_modified = todo_check[
                            # todo_check["LastUpdatedAt"].notna() & (todo_check["LastUpdatedAt"] > recent_threshold)
                        # ].copy()
                        # recent_count = len(recently_modified)
                        # if recent_count > 0:
                            # print(f"   → {recent_count} taken gewijzigd in deze sessie")
                            # needs_save = True
                            # modified_count = max(modified_count, recent_count)
                # except Exception as e:
                    # print(f"   ⚠️ Kon LastUpdatedAt niet checken: {e}")
                    # import traceback as _tb; _tb.print_exc()

                # if needs_save and modified_count > 0:
                    # rows_to_save = recently_modified.copy()
                    # # ensure expected cols
                    # expected_cols = [
                        # "TaskID", "staffGID", "staffSAPNR", "MedewerkerID", "MedewerkerNaam",
                        # "CostCenter", "CertName", "CertName_norm", "TaskType", "Status",
                        # "Status_Detail", "Nodig", "Commentaar", "Ingeschreven_Datum",
                        # "Ingeschreven_Locatie", "CreatedAt", "LastUpdatedAt", "CreatedBy",
                        # "Geldigheid_maanden", "ExpiryDate", "DaysUntilExpiry"
                    # ]
                    # for c in expected_cols:
                        # if c not in rows_to_save.columns:
                            # rows_to_save[c] = None

                    # # Normalize TaskID empties to None
                    # rows_to_save["TaskID"] = rows_to_save["TaskID"].apply(
                        # lambda x: None if (pd.isna(x) or str(x).strip().lower() == "nan" or str(x).strip() == "") else x
                    # )

                    # # Add unique SrcRowId for mapping
                    # import uuid
                    # rows_to_save["_SrcRowId"] = [str(uuid.uuid4()) for _ in range(len(rows_to_save))]

                    # print(f"\n   💾 Opslaan subset: {len(rows_to_save)} gewijzigde taken naar SQL...")
                    # if self.USE_SQL_FOR_TODO and getattr(self, "sql_training_manager", None):
                        # try:
                            # # call the updated save that returns mapping
                            # success, mapping = self.sql_training_manager.save_todo_planner(rows_to_save)
                            # if success:
                                # print("   ✅ Subset todo opgeslagen naar SQL Server")
                                # # Apply mapping to in-memory DF: map _SrcRowId -> TaskID and update self.df['todo']
                                # try:
                                    # # Update rows_to_save with TaskID from mapping
                                    # rows_to_save["TaskID"] = rows_to_save["_SrcRowId"].map(lambda k: mapping.get(k))
                                    # # For each saved row, find matching row(s) in self.df['todo'] by natural key and LastUpdatedAt (or by _SrcRowId if kept)
                                    # # Simplest: join on staffGID, CertName_norm, TaskType and LastUpdatedAt if needed
                                    # todo_df = self.df.get("todo", pd.DataFrame()).copy()
                                    # merged = pd.merge(
                                        # todo_df,
                                        # rows_to_save[["_SrcRowId", "TaskID", "staffGID", "CertName_norm", "TaskType", "LastUpdatedAt"]],
                                        # how="left",
                                        # left_on=["staffGID", "CertName_norm", "TaskType", "LastUpdatedAt"],
                                        # right_on=["staffGID", "CertName_norm", "TaskType", "LastUpdatedAt"],
                                        # suffixes=("", "_new")
                                    # )
                                    # # If TaskID_new present, set TaskID
                                    # if "TaskID_new" in merged.columns:
                                        # merged["TaskID"] = merged["TaskID_new"].fillna(merged["TaskID"])
                                        # merged.drop(columns=[c for c in merged.columns if c.endswith("_new")], inplace=True)
                                        # self.df["todo"] = merged
                                    # else:
                                        # # fallback: try mapping by SrcRowId via iterative update
                                        # for idx, r in rows_to_save.iterrows():
                                            # srcid = r["_SrcRowId"]
                                            # new_tid = mapping.get(srcid)
                                            # if new_tid:
                                                # mask = (
                                                    # (self.df["todo"]["staffGID"] == r["staffGID"]) &
                                                    # (self.df["todo"]["CertName_norm"] == r["CertName_norm"]) &
                                                    # (self.df["todo"]["TaskType"] == r["TaskType"]) &
                                                    # (pd.to_datetime(self.df["todo"].get("LastUpdatedAt", pd.NaT)) == pd.to_datetime(r["LastUpdatedAt"]))
                                                # )
                                                # self.df["todo"].loc[mask, "TaskID"] = new_tid
                                # except Exception as e:
                                    # print(f"   ⚠️ Kon TaskID mapping niet toepassen: {e}")
                                    # import traceback as _tb; _tb.print_exc()
                            # else:
                                # print("   ⚠️ Subset save gaf False")
                                # self.errors.append("TODO subset save faalde (returned False)")
                        # except Exception as e:
                            # print("   ❌ Exception tijdens save_todo_planner:", e)
                            # import traceback as _tb; _tb.print_exc()
                            # self.errors.append(f"Subset-save exception: {e}")
                    # else:
                        # print("   ❌ Kan TODO niet opslaan (SQL niet beschikbaar)")
                        # self.errors.append("SQL niet beschikbaar voor TODO save")
                # else:
                    # print("   ✅ Geen wijzigingen - SKIP SAVE")
            # else:
                # print("   ⚠️ Geen todo data om op te slaan")
        # except Exception as e:
            # print(f"   ⚠️ Fout tijdens STAP 15 opslaan: {e}")
            # import traceback as _tb; _tb.print_exc()
            # self.errors.append(f"Fout tijdens opslaan: {e}")
        
        
        # # # STAP 16: check training_req tegen config (Stille controle)
        # # try:
            # # print("\n🔍 STAP 16: Controleren op inschrijvingen zonder config...")
            # # check_result = self.check_training_req_against_config()
            # # missing_count = check_result.get("missing_count", 0) if isinstance(check_result, dict) else 0
            
            # # if missing_count > 0:
                # # # We tonen dit alleen in de zwarte console, NIET in een popup
                # # print(f"   ℹ️ INFO: {missing_count} inschrijving(en) gevonden die nog niet in config staan.")
                # # print(f"   ℹ️ De tool heeft deze zojuist in STAP 13 automatisch hersteld in de database.")
                
                # # # De lijst met namen printen we ook alleen naar de console voor jouw debug-info
                # # for item in check_result.get("missing_items", [])[:3]:
                    # # print(f"      • {item.get('medewerker')} → {item.get('cert_name')}")
                
                # # # Zorg dat de self.errors.append hieronder NIET meer staat.
                # # # Als die weg is, krijgt de gebruiker geen popup te zien.
            # # else:
                # # print("   ✅ Alle inschrijvingen zijn correct gesynchroniseerd met de config.")
                
        # # except Exception as e:
            # # # Alleen bij een echte systeemcrash loggen we het nog naar de console
            # # print(f"   ⚠️ Debug: Fout bij achtergrond-check training_req: {e}")
        # # # STAP 17: Repareer namen (Nieuw)
        # # #try:
        # # #    self.clean_sql_config_names()
        # # #except Exception as e:
        # # #    print(f"⚠️ Config cleanup fout: {e}")
        # # # -------------------------
        
        # # # Return summary
        # # print("\n" + "="*60)
        # # print(f"✅ load_all() VOLTOOID - Costcenter: {costcenter_filter or 'ALLE'}")
        # # print("=" * 60)
        # # staff_count = len(self.df.get("staff", pd.DataFrame()))
        # # cert_count = len(self.df.get("certificates", pd.DataFrame()))
        # # todo_count = len(self.df.get("todo", pd.DataFrame()))
        # # print(f"   📊 Samenvatting:")
        # # print(f"      • Staff: {staff_count} medewerkers")
        # # print(f"      • Certificates: {cert_count} rijen")
        # # print(f"      • Todo: {todo_count} taken")
        # # if self.errors:
            # # print(f"\n   ⚠️ {len(self.errors)} waarschuwing(en):")
            # # for err in self.errors[:20]:
                # # print(f"      - {err}")
        # # print("=" * 60 + "\n")
        
        
        # # return True

    # ══════════════════════════════════════════════════════════════
    # 🆕 SQL HELPER METHODS (V11)
    # ══════════════════════════════════════════════════════════════
    
    def _load_staff_from_excel(self) -> pd.DataFrame:
        """Laad staff uit Excel (fallback methode)."""
        try:
            staff_file = INPUT_FILES.get("staff")
            
            if not staff_file:
                print("   ❌ Geen STAFF Excel bestand geconfigureerd")
                return pd.DataFrame()
            
            if not staff_file.exists():
                print(f"   ❌ STAFF Excel niet gevonden: {staff_file}")
                return pd.DataFrame()
            
            print(f"   → Excel: {staff_file.name}")
            staff = pd.read_excel(staff_file)
            
            for col in staff.select_dtypes(include=['object']).columns:
                staff[col] = staff[col].astype(str).str.strip()
            
            if 'staffGID' in staff.columns:
                staff['staffGID'] = staff['staffGID'].astype(str).str.strip()
            if 'staffSAPNR' in staff.columns:
                staff['staffSAPNR'] = staff['staffSAPNR'].apply(normalize_sapnr)
            
            if "staffFIRSTNAME" in staff.columns and "staffLASTNAME" in staff.columns:
                staff["FullName"] = (
                    staff["staffLASTNAME"].astype(str).str.strip() + ", " + 
                    staff["staffFIRSTNAME"].astype(str).str.strip()
                )
                print("   → FullName aangemaakt als 'Achternaam, Voornaam'")
            elif "FullName" not in staff.columns:
                name_col = None
                for c in ["Name+Firstname", "FullName", "Naam", "Employee_Name"]:
                    if c in staff.columns:
                        name_col = c
                        break
                
                if name_col:
                    staff["FullName"] = staff[name_col]
                    print(f"   → FullName overgenomen van {name_col}")
            
            for col in staff.columns:
                if staff[col].dtype == 'object':
                    staff[col] = staff[col].replace(['nan', 'NaN', 'NAN', 'None'], pd.NA)
            
            print(f"   ✅ Excel: {len(staff)} medewerkers geladen")
            return staff
            
        except FileNotFoundError as e:
            print(f"   ❌ Excel bestand niet gevonden: {e}")
            return pd.DataFrame()
        except PermissionError as e:
            print(f"   ❌ Geen toegang tot Excel bestand: {e}")
            print(f"   💡 Sluit het bestand als het geopend is in Excel")
            return pd.DataFrame()
        except Exception as e:
            print(f"   ❌ Excel laden fout: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    # ══════════════════════════════════════════════════════════════
    # 🔧 AANVULLENDE METHODS (ONTBRAKEN IN VORIGE VERSIE)
    # ══════════════════════════════════════════════════════════════
    
    def check_training_req_against_config(self) -> dict:
        """
        Controleert of alle inschrijvingen in Training_Req voorkomen in config.
        
        Returns:
            dict met:
            - "missing_count": aantal ontbrekende inschrijvingen
            - "missing_items": list van dict met medewerker/certificaat details
            - "details_html": HTML formatted details voor email
        """
        result = {
            "missing_count": 0,
            "missing_items": [],
            "details_html": ""
        }
        
        training_req = self.df.get("training_req", pd.DataFrame())
        config_cert = self.df.get("config_cert", pd.DataFrame())
        staff = self.df.get("staff", pd.DataFrame())
        
        if training_req.empty or "CertName" not in training_req.columns:
            return result
        
        id_col = self.get_id_column() or "staffGID"
        
        if id_col in training_req.columns:
            training_req = training_req.copy()
            training_req[id_col] = training_req[id_col].astype(str).str.strip()
        
        if not config_cert.empty and id_col in config_cert.columns:
            config_cert = config_cert.copy()
            config_cert[id_col] = config_cert[id_col].astype(str).str.strip()
            config_cert["CertName"] = config_cert["CertName"].astype(str).str.strip()
        
        missing_items = []
        
        for idx, row in training_req.iterrows():
            staff_id = str(row.get(id_col, "")).strip()
            cert_name = str(row.get("CertName", "")).strip()
            
            if not staff_id or not cert_name:
                continue
            
            if config_cert.empty:
                is_in_config = False
            else:
                is_in_config = (
                    (config_cert[id_col] == staff_id) & 
                    (config_cert["CertName"] == cert_name)
                ).any()
            
            if not is_in_config:
                medewerker_naam = str(row.get("MedewerkerNaam", "")).strip()
                if not medewerker_naam or medewerker_naam.lower() == "nan":
                    if not staff.empty and id_col in staff.columns:
                        staff_row = staff[staff[id_col].astype(str).str.strip() == staff_id]
                        if not staff_row.empty:
                            for name_col in ["FullName", "Name+Firstname", "Naam"]:
                                if name_col in staff_row.columns:
                                    medewerker_naam = str(staff_row.iloc[0][name_col]).strip()
                                    break
                
                scheduled_date = row.get("ScheduledDate", row.get("ScheduledDateParsed", pd.NaT))
                location = str(row.get("Location", "") or "").strip()
                status = str(row.get("RequestStatus", "") or "").strip()
                costcenter = str(row.get("CostCenter", "") or "").strip()
                
                date_str = ""
                if pd.notna(scheduled_date):
                    try:
                        dt = pd.to_datetime(scheduled_date)
                        date_str = dt.strftime("%d-%m-%Y")
                    except:
                        pass
                
                missing_items.append({
                    "staff_id": staff_id,
                    "medewerker": medewerker_naam or staff_id,
                    "cert_name": cert_name,
                    "scheduled_date": date_str,
                    "location": location,
                    "status": status,
                    "costcenter": costcenter,
                })
        
        result["missing_count"] = len(missing_items)
        result["missing_items"] = missing_items
        
        if missing_items:
            from datetime import datetime
            timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            count = len(missing_items)
            
            html = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: 'Segoe UI', Arial, sans-serif; }}
                    h2 {{ color: #dc3545; }}
                    table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                    th {{ background-color: #dc3545; color: white; padding: 10px; text-align: left; }}
                    td {{ border: 1px solid #ddd; padding: 8px; }}
                    tr:nth-child(even) {{ background-color: #f9f9f9; }}
                    .warning {{ background-color: #fff3cd; padding: 15px; border-left: 5px solid #ffc107; margin: 20px 0; }}
                </style>
            </head>
            <body>
                <h2>⚠️ Ingeschreven Opleidingen Zonder Config</h2>
                
                <div class="warning">
                    <strong>Let op! </strong> Er zijn <strong>{count}</strong> inschrijving(en) gevonden in Training_Req 
                    die <strong>NIET</strong> in de Medewerker_Certificaat_Config staan.
                    <br><br>
                    Dit betekent dat deze opleidingen mogelijk niet als 'Nodig' zijn gemarkeerd 
                    en dus niet in de planner verschijnen! 
                </div>
                
                <table>
                    <thead>
                        <tr>
                            <th>Medewerker</th>
                            <th>ID</th>
                            <th>Certificaat</th>
                            <th>Datum</th>
                            <th>Locatie</th>
                            <th>Status</th>
                            <th>Costcenter</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            for item in missing_items:
                html += f"""
                        <tr>
                            <td>{item['medewerker']}</td>
                            <td>{item['staff_id']}</td>
                            <td><strong>{item['cert_name']}</strong></td>
                            <td>{item['scheduled_date'] or '-'}</td>
                            <td>{item['location'] or '-'}</td>
                            <td>{item['status'] or '-'}</td>
                            <td>{item['costcenter'] or '-'}</td>
                        </tr>
                """
            
            html += f"""
                    </tbody>
                </table>
                
                <h3>📋 Aanbevolen Actie</h3>
                <ol>
                    <li>Open de Training Management applicatie</li>
                    <li>Ga naar <strong>Medewerkerbeheer</strong></li>
                    <li>Zoek de betreffende medewerker(s) op</li>
                    <li>Voeg het certificaat toe en zet <strong>"Nodig"</strong> aan</li>
                    <li>Klik op <strong>"Opslaan"</strong></li>
                </ol>
                
                <p style="color: #6c757d; font-size: 11px; margin-top: 30px;">
                    Deze email is automatisch gegenereerd door Training Management System V11.<br>
                    Timestamp: {timestamp}
                </p>
            </body>
            </html>
            """
            
            result["details_html"] = html
        
        return result

    def load_competences(self):
        """Laad competenties uit Excel (legacy)."""
        path = INPUT_FILES.get("competences")
        df = pd.read_excel(path)

        df = df.rename(columns={
            "CompName": "Competence",
            "Achieved_On": "ValidatedAt",
            "Valid_Until": "ValidUntil",
            "Remark": "Comment",
            "Employee_Name": "Employee"
        })

        norm = self.normalize_certname
        if "Competence" in df.columns:
            df["Competence"] = df["Competence"].astype(str).str.strip()
            df["Competence_norm"] = df["Competence"].apply(norm)

        id_col = self.get_id_column() or "staffGID"
        if id_col in df.columns:
            df[id_col] = df[id_col].astype(str).str.strip()

        self.df["competences"] = df

    # def sync_competence_tasks(self):
        # """
        # ULTIMATE VERSION: sync_competence_tasks.
        # Combineert V10 (volledige functionaliteit) met V11 (afdelingsbeveiliging).
        # Voorkomt 'NameError: my_department_gids' en 'Invalid column name: Competence'.
        # """
        # print("\n" + "="*60)
        # print(f"🔄 sync_competence_tasks() - AFDELING: {self.active_costcenter}")
        # print("="*60)
        
        # # 1. LAAD BENODIGDE DATA
        # cfg = self.df.get("competence_config", pd.DataFrame())
        # staff = self.df.get("staff", pd.DataFrame())
        # competences = self.df.get("competences", pd.DataFrame())
        # todo = self.df.get("todo", pd.DataFrame())
        
        # if cfg.empty or staff.empty:
            # print("   ⚠️ Geen competentie config of staff data - niets te doen")
            # return
        
        # # 2. BEPAAL GIDS VAN DE ACTIEVE AFDELING (De lek-dichting)
        # id_col = self.get_id_column() or "staffGID"
        # # We gebruiken 'valid_staff_ids' om consistent te blijven met load_all
        # valid_staff_ids = set(staff[id_col].astype(str).str.strip().unique())
        
        # cfg_id_col = id_col if id_col in cfg.columns else "staffGID"
        # def is_true(x): return str(x).lower() in ['true', '1', 'ja', 'yes', 't']

        # # Filter config: Alleen jouw mensen van dit CC & alleen als 'Nodig' aan staat
        # cfg_filtered = cfg[cfg[cfg_id_col].astype(str).str.strip().isin(valid_staff_ids)].copy()
        # cfg_nodig = cfg_filtered[cfg_filtered["Nodig"].apply(is_true)].copy()
        
        # print(f"   📊 Config voor {self.active_costcenter}: {len(cfg_nodig)} rijen met Nodig=Aan.")
        
        # if cfg_nodig.empty:
            # print(f"   ✅ Geen vaardigheden met Nodig=True voor dit costcenter")
            # return

        # # 3. LOOKUPS BOUWEN (Alleen voor relevante medewerkers)
        # comp_lookup = {}
        # if not competences.empty:
            # comp_id_col = id_col if id_col in competences.columns else "staffGID"
            # comp_name_col = next((c for c in ["Competence", "CompName", "Vaardigheid"] if c in competences.columns), "Competence")
            # for _, row in competences.iterrows():
                # sid = str(row.get(comp_id_col, "")).strip()
                # if sid in valid_staff_ids:
                    # cnorm = self.normalize_certname(str(row.get(comp_name_col, "")))
                    # comp_lookup[(sid, cnorm)] = row

        # existing_tasks = set()
        # if not todo.empty:
            # t_id = id_col if id_col in todo.columns else "staffGID"
            # for _, row in todo.iterrows():
                # sid = str(row.get(t_id, "")).strip()
                # if sid in valid_staff_ids:
                    # cnorm = self.normalize_certname(str(row.get("CertName", "")))
                    # stat = str(row.get("Status", "")).lower()
                    # if str(row.get("TaskType", "")).lower() == "vaardigheid":
                        # if stat not in ["afgewerkt", "closed", "gesloten"]:
                            # existing_tasks.add((sid, cnorm))

        # staff_lookup = {}
        # for _, row in staff.iterrows():
            # sid = str(row.get(id_col, "")).strip()
            # staff_lookup[sid] = {
                # "name": str(row.get("MedewerkerNaam", row.get("FullName", ""))).strip(),
                # "cc": str(row.get("staffCOSTCENTER315", row.get("CostCenter", ""))).strip(),
                # "sap": str(row.get("staffSAPNR", "")).strip()
            # }

        # # 4. VERWERK & GENEREER (Met alle V10 stats)
        # new_tasks = []
        # stats = {"nieuw": 0, "renewal": 0, "skip_actief": 0, "skip_taak_bestaat": 0, "skip_geen_staff": 0, "naam_genormaliseerd": 0}
        
        # now = datetime.now()
        # threshold_days = 180
        # comp_col = "Competence" if "Competence" in cfg_nodig.columns else "CertName"

        # for idx, row in cfg_nodig.iterrows():
            # sid = str(row.get(cfg_id_col, "")).strip()
            # comp_name_original = str(row.get(comp_col, "")).strip()
            # comp_name = self.normalize_certname(comp_name_original)
            
            # if comp_name != comp_name_original.lower(): # Kleine indicatie van normalisatie
                # stats["naam_genormaliseerd"] += 1
            
            # # Veiligheid: dubbele check of ID in onze CC lijst zit
            # if sid not in valid_staff_ids:
                # stats["skip_geen_staff"] += 1
                # continue
            
            # if (sid, comp_name) in existing_tasks:
                # stats["skip_taak_bestaat"] += 1
                # continue

            # s_info = staff_lookup.get(sid)
            # if not s_info: 
                # stats["skip_geen_staff"] += 1
                # continue

            # # Status bepaling
            # status, detail, expiry, days = "Open", "Vaardigheid nog niet behaald", None, None
            # exist = comp_lookup.get((sid, comp_name))
            
            # if exist is not None:
                # exp_col = next((c for c in ["ValidUntil", "ExpiryDate", "Valid_Until"] if c in exist.index), None)
                # if exp_col and pd.notna(exist[exp_col]):
                    # val_str = str(exist[exp_col]).lower()
                    
                    # # 🔥 FIX: Als er "onbeperkt" staat, is de competentie geldig -> SKIP taak
                    # if "onbeperkt" in val_str or "unlimited" in val_str:
                        # stats["skip_actief"] += 1
                        # continue
                    
                    # try:
                        # expiry = pd.to_datetime(exist[exp_col], errors='coerce')
                        # if pd.notna(expiry):
                            # days = int((expiry - now).days)
                            # if days > threshold_days: 
                                # stats["skip_actief"] += 1
                                # continue 
                            # status, detail = "Open", f"Vernieuwing nodig ({days} dagen)"
                            # stats["renewal"] += 1
                    # except Exception as e:
                        # # Als de datum-conversie mislukt, loggen we het en gaan we door
                        # print(f"⚠️ Datumfout bij {sid} - {comp_name}: {e}")
                        # status, detail = "Open", "Check datum formaat"
                # else:
                    # # Gevonden in behaald maar geen verloopdatum -> OK, niet toevoegen
                    # stats["skip_actief"] += 1
                    # continue 

            # # Taak toevoegen (Dit staat nu BUITEN de try-except van de datum)
            # new_tasks.append({
                # "staffGID": sid, 
                # "staffSAPNR": s_info["sap"], 
                # "MedewerkerID": sid,
                # "MedewerkerNaam": s_info["name"], 
                # "CostCenter": s_info["cc"],
                # "CertName": comp_name_original, 
                # "CertName_norm": comp_name, 
                # "TaskType": "Vaardigheid",
                # "Status": status, 
                # "Status_Detail": detail, 
                # "Nodig": True,
                # "Strategisch": row.get("Strategisch", False),
                # "Commentaar": row.get("Commentaar", "") or row.get("Opmerking", ""),
                # "ExpiryDate": expiry, 
                # "DaysUntilExpiry": days,
                # "CreatedAt": now, 
                # "LastUpdatedAt": now, 
                # "CreatedBy": "sync_competence_tasks"
            # })
            # stats["nieuw"] += 1
 

        # # 5. SAMENVOEGEN & OPSLAAN
        # if new_tasks:
            # self.df["todo"] = pd.concat([todo, pd.DataFrame(new_tasks)], ignore_index=True)
            # if self.USE_SQL_FOR_TODO: 
                # self.save_todo_planner()
        
        # # 6. VOLLEDIGE V10 RAPPORTAGE
        # print(f"\n✅ sync_competence_tasks() RESULTAAT:")
        # print(f"   • Nieuwe vaardigheden: {stats['nieuw']}")
        # print(f"   • Vernieuwingen nodig: {stats['renewal']}")
        # print(f"   • Overgeslagen (nog geldig): {stats['skip_actief']}")
        # print(f"   • Overgeslagen (taak bestaat): {stats['skip_taak_bestaat']}")
        # print(f"   • Overgeslagen (niet in costcenter): {stats['skip_geen_staff']}")
        # print(f"   • 🔄 Namen genormaliseerd: {stats['naam_genormaliseerd']}")
        # print(f"   • TOTAAL NIEUWE TAKEN: {len(new_tasks)}")
        # print("="*60)
    def sync_competence_tasks(self):
        """
        ULTIMATE VERSION:  sync_competence_tasks. 
        Combineert V10 (volledige functionaliteit) met V11 (afdelingsbeveiliging).
        Voorkomt 'NameError:  my_department_gids' en 'Invalid column name:  Competence'. 
        """
        print("\n" + "="*60)
        print(f"🔄 sync_competence_tasks() - AFDELING: {self. active_costcenter}")
        print("="*60)
        
        # 1. LAAD BENODIGDE DATA
        cfg = self.df. get("competence_config", pd.DataFrame())
        staff = self.df.get("staff", pd. DataFrame())
        competences = self.df. get("competences", pd.DataFrame())
        todo = self.df.get("todo", pd. DataFrame())
        
        if cfg.empty or staff.empty:
            print("   ⚠️ Geen competentie config of staff data - niets te doen")
            return
        
        # 2. BEPAAL GIDS VAN DE ACTIEVE AFDELING
        id_col = self.get_id_column() or "staffGID"
        valid_staff_ids = set(staff[id_col].astype(str).str.strip().unique())
        
        cfg_id_col = id_col if id_col in cfg.columns else "staffGID"
        def is_true(x): return str(x).lower() in ['true', '1', 'ja', 'yes', 't']

        # Filter config:  Alleen jouw mensen van dit CC & alleen als 'Nodig' aan staat
        cfg_filtered = cfg[cfg[cfg_id_col].astype(str).str.strip().isin(valid_staff_ids)].copy()
        cfg_nodig = cfg_filtered[cfg_filtered["Nodig"]. apply(is_true)].copy()
        
        print(f"   📊 Config voor {self.active_costcenter}:  {len(cfg_nodig)} rijen met Nodig=Aan.")
        
        if cfg_nodig. empty:
            print(f"   ✅ Geen vaardigheden met Nodig=True voor dit costcenter")
            return

        # 3. LOOKUPS BOUWEN
        comp_lookup = {}
        if not competences.empty:
            comp_id_col = id_col if id_col in competences. columns else "staffGID"
            comp_name_col = next((c for c in ["Competence", "CompName", "Vaardigheid"] if c in competences. columns), "Competence")
            for _, row in competences. iterrows():
                sid = str(row. get(comp_id_col, "")).strip()
                if sid in valid_staff_ids: 
                    cnorm = self.normalize_certname(str(row.get(comp_name_col, "")))
                    comp_lookup[(sid, cnorm)] = row

        existing_tasks = set()
        if not todo.empty:
            t_id = id_col if id_col in todo.columns else "staffGID"
            for _, row in todo. iterrows():
                sid = str(row.get(t_id, "")).strip()
                if sid in valid_staff_ids:
                    cnorm = self.normalize_certname(str(row.get("CertName", "")))
                    stat = str(row.get("Status", "")).lower()
                    if str(row.get("TaskType", "")).lower() == "vaardigheid":
                        if stat not in ["afgewerkt", "closed", "gesloten"]: 
                            existing_tasks.add((sid, cnorm))

        staff_lookup = {}
        for _, row in staff. iterrows():
            sid = str(row.get(id_col, "")).strip()
            staff_lookup[sid] = {
                "name": str(row.get("MedewerkerNaam", row.get("FullName", ""))).strip(),
                "cc": str(row.get("staffCOSTCENTER315", row.get("CostCenter", ""))).strip(),
                "sap": str(row.get("staffSAPNR", "")).strip()
            }

        # 4. VERWERK & GENEREER
        new_tasks = []
        stats = {"nieuw": 0, "renewal": 0, "skip_actief":  0, "skip_taak_bestaat": 0, "skip_geen_staff": 0, "naam_genormaliseerd": 0}
        
        now = datetime.now()
        threshold_days = 180
        comp_col = "Competence" if "Competence" in cfg_nodig. columns else "CertName"

        for idx, row in cfg_nodig.iterrows():
            sid = str(row.get(cfg_id_col, "")).strip()
            comp_name_original = str(row.get(comp_col, "")).strip()
            comp_name = self. normalize_certname(comp_name_original)
            
            if comp_name != comp_name_original. lower():
                stats["naam_genormaliseerd"] += 1
            
            # Veiligheid:  dubbele check of ID in onze CC lijst zit
            if sid not in valid_staff_ids:
                stats["skip_geen_staff"] += 1
                continue
            
            if (sid, comp_name) in existing_tasks:
                stats["skip_taak_bestaat"] += 1
                continue

            s_info = staff_lookup.get(sid)
            if not s_info:  
                stats["skip_geen_staff"] += 1
                continue

            # Status bepaling
            status, detail, expiry, days = "Open", "Vaardigheid nog niet behaald", None, None
            exist = comp_lookup.get((sid, comp_name))
            
            if exist is not None:
                # Competentie GEVONDEN in behaald - check vervaldatum
                exp_col = next((c for c in ["ValidUntil", "ExpiryDate", "Valid_Until"] if c in exist. index), None)
                if exp_col and pd.notna(exist[exp_col]):
                    val_str = str(exist[exp_col]).lower()
                    
                    # Als er "onbeperkt" staat, is de competentie geldig -> SKIP taak
                    if "onbeperkt" in val_str or "unlimited" in val_str:
                        stats["skip_actief"] += 1
                        continue
                    
                    try:
                        expiry = pd.to_datetime(exist[exp_col], errors='coerce')
                        if pd. notna(expiry):
                            days = int((expiry - now).days)
                            if days > threshold_days:  
                                stats["skip_actief"] += 1
                                continue 
                            status, detail = "Open", f"Vernieuwing nodig ({days} dagen)"
                            stats["renewal"] += 1
                    except Exception as e: 
                        print(f"⚠️ Datumfout bij {sid} - {comp_name}:  {e}")
                        status, detail = "Open", "Check datum formaat"
                else:
                    # Gevonden in behaald maar geen verloopdatum -> competentie is geldig
                    stats["skip_actief"] += 1
                    continue
            else: 
                # Competentie NIET gevonden in behaald -> taak nodig!
                status = "Open"
                detail = "Nieuwe vaardigheid nodig"

            # Taak toevoegen
            new_tasks.append({
                "staffGID": sid, 
                "staffSAPNR": s_info["sap"], 
                "MedewerkerID": sid,
                "MedewerkerNaam":  s_info["name"], 
                "CostCenter": s_info["cc"],
                "CertName": comp_name_original, 
                "CertName_norm": comp_name, 
                "TaskType": "Vaardigheid",
                "Status": status, 
                "Status_Detail": detail, 
                "Nodig": True,
                "Strategisch":  row.get("Strategisch", False),
                "Commentaar":  row.get("Commentaar", "") or row.get("Opmerking", ""),
                "ExpiryDate": expiry, 
                "DaysUntilExpiry": days,
                "CreatedAt": now, 
                "LastUpdatedAt":  now, 
                "CreatedBy": "sync_competence_tasks"
            })
            stats["nieuw"] += 1

        # 5. SAMENVOEGEN & OPSLAAN
        if new_tasks: 
            self.df["todo"] = pd.concat([todo, pd.DataFrame(new_tasks)], ignore_index=True)
            if self.USE_SQL_FOR_TODO:  
                self. save_todo_planner()
        
        # 6. VOLLEDIGE RAPPORTAGE
        print(f"\n✅ sync_competence_tasks() RESULTAAT:")
        print(f"   • Nieuwe vaardigheden: {stats['nieuw']}")
        print(f"   • Vernieuwingen nodig: {stats['renewal']}")
        print(f"   • Overgeslagen (nog geldig): {stats['skip_actief']}")
        print(f"   • Overgeslagen (taak bestaat): {stats['skip_taak_bestaat']}")
        print(f"   • Overgeslagen (niet in costcenter): {stats['skip_geen_staff']}")
        print(f"   • 🔄 Namen genormaliseerd:  {stats['naam_genormaliseerd']}")
        print(f"   • TOTAAL NIEUWE TAKEN: {len(new_tasks)}")
        print("="*60)
    
    def _append_comment(self, existing_comment: Any, new_info: str) -> str:
        """
        Helper method to safely append new information to existing comment.
        Handles NaN, None, empty strings, and other null representations.
        
        Args:
            existing_comment: The existing comment value (may be NaN, None, or string)
            new_info: The new information to append
            
        Returns:
            The combined comment string
        """
        # Check if existing comment is NaN before converting to string
        if pd.isna(existing_comment):
            return new_info
        
        # Convert to string and clean
        comment_str = str(existing_comment).strip()
        
        # Check if we have valid existing content
        if comment_str and comment_str.lower() not in ("nan", "none", ""):
            return f"{comment_str} | {new_info}"
        else:
            return new_info
    
    
 
    def add_medewerker_config(self, staff_id: str, cert_name: str, nodig: bool = True) -> bool:
        """
        Voegt een certificaat toe aan de configuratie van een medewerker.
        Roept de SQL manager aan om de MERGE (Upsert) veilig uit te voeren.
        """
        import pandas as pd
        
        # 1. Update SQL (via de manager die we hebben gefixt met de juiste MERGE logica)
        success = False
        if self.sql_training_manager:
            success = self.sql_training_manager.add_medewerker_config(
                staff_id=staff_id, 
                cert_name=cert_name, 
                nodig=nodig
            )

        # 2. Update de lokale lijst zodat de UI direct bijgewerkt is zonder herstart
        if success:
            staff_df = self.df.get("staff", pd.DataFrame())
            m_naam = f"Onbekend ({staff_id})"
            if not staff_df.empty:
                match = staff_df[staff_df['staffGID'] == staff_id]
                if not match.empty:
                    r = match.iloc[0]
                    m_naam = f"{r.get('staffLASTNAME', '')}, {r.get('staffFIRSTNAME', '')}".strip(", ")

            # Voeg toe aan de lokale dataframe config_cert
            df_cfg = self.df.get("config_cert", pd.DataFrame())
            new_row = {
                "staffGID": staff_id, 
                "MedewerkerNaam": m_naam, 
                "CertName": cert_name,
                "CertName_norm": self.normalize_certname(cert_name), 
                "Nodig": nodig,
                "Strategisch": False, 
                "Interval_maanden": 0, 
                "LaatsteWijziging": pd.Timestamp.now()
            }
            self.df["config_cert"] = pd.concat([df_cfg, pd.DataFrame([new_row])], ignore_index=True)
            
        return success
    
   
    # --- HELPER FUNCTIE ---
    def get_expiry_for_member(self, gid, cert_norm):
        """Helper om snel de verloopdatum van een certificaat te vinden."""
        certs = self.df.get("certificates", pd.DataFrame())
        if certs.empty: return {}
        
        # Zoek laatste match op GID en genormaliseerde naam
        match = certs[(certs['staffGID'] == gid) & (certs['CertName_norm'] == cert_norm)]
        if not match.empty:
            match = match.sort_values('Expiry_Date', ascending=False).iloc[0]
            exp_date = pd.to_datetime(match['Expiry_Date'])
            days = (exp_date - pd.Timestamp.now().normalize()).days
            return {"date": exp_date, "days": days}
        return {}

    def sync_inschrijvingen(self):
        """
        V21:  STRIKTE AFDELINGS-SYNC.  
        Zorgt dat alleen inschrijvingen voor de EIGEN afdeling worden verwerkt.
        Voorkomt dat mensen van B1, B3, B4 etc. in de planner belanden.
        
        V21-FIX:  CostCenter lookup aangepast om te werken na kolom-hernoemen in load_all().
        """
        print("\n" + "="*60)
        print(f"🛡️ sync_inschrijvingen() - Filter: {self.active_costcenter or 'GEEN'}")
        print("="*60)
        
        training_req = self.df. get("training_req", pd.DataFrame())
        todo = self.df.get("todo", pd. DataFrame())
        staff = self.df.get("staff", pd. DataFrame())
        mapping = self.df. get("mapping_cert", pd.DataFrame())
        
        if training_req.empty or staff.empty:
            print("   ⚠️ Geen Xaurum-inschrijvingen of staff gevonden om te synchroniseren.")
            return 0
            
        # 1. Maak een set van GIDs die JOUW afdeling zijn (gebaseerd op geladen staff)
        my_department_gids = set(staff["staffGID"].astype(str).str.strip().unique())
        
        # 2. Bouw lookup voor verrijking (alleen voor jouw mensen)
        # V21-FIX:  Zoek CostCenter in beide mogelijke kolomnamen + fallback
        staff_lookup = {}
        for _, s_row in staff.iterrows():
            sid = str(s_row.get("staffGID", "")).strip()
            
            # FIX: Zoek CostCenter in beide mogelijke kolomnamen + fallback naar active_costcenter
            cc_value = (
                s_row.get("CostCenter", "") or 
                s_row.get("staffCOSTCENTER315", "") or 
                self.active_costcenter or 
                ""
            )
            
            staff_lookup[sid] = {
                "sapnr": str(s_row.get("staffSAPNR", "")).replace(".0", ""),
                "costcenter": str(cc_value).strip(),
                "name":  str(s_row.get("MedewerkerNaam", s_row. get("FullName", sid))).strip()
            }

        translation_map = {}
        if not mapping.empty:
            src = next((c for c in ["OrigineleNaam", "Frans"] if c in mapping. columns), mapping.columns[0])
            dst = next((c for c in ["VertaaldeNaam", "Nederlands"] if c in mapping.columns), mapping.columns[1])
            for _, row in mapping.iterrows():
                translation_map[self.normalize_certname(str(row[src]))] = str(row[dst]).strip()

        # 3. FILTER de Xaurum-lijst VOORAF zodat we andere afdelingen negeren
        # Dit is de 'Kraan' die we dichtdraaien voor B1, B3, B4 etc. 
        id_req_col = "staffGID" if "staffGID" in training_req.columns else "User ID"
        my_reqs = training_req[training_req[id_req_col].astype(str).str.strip().isin(my_department_gids)]. copy()

        if my_reqs.empty:
            print(f"   ✅ Geen inschrijvingen gevonden voor de {len(my_department_gids)} mensen van deze afdeling.")
            return 0

        print(f"   🎯 Verwerken van {len(my_reqs)} relevante inschrijvingen voor {self.active_costcenter}...")

        updates = 0
        new_tasks_count = 0
        now = pd.Timestamp.now()
        todo_work = todo. copy()

        # 4. Loop alleen door de RELEVANTE inschrijvingen
        for _, req_row in my_reqs.iterrows():
            staff_id = str(req_row. get(id_req_col, "")).strip()
            raw_name = str(req_row.get("CertName", req_row.get("Item Description", ""))).strip()
            if not raw_name or raw_name. lower() == 'nan':  continue
            
            # Normalisatie en vertaling
            cert_norm = self.normalize_certname(raw_name)
            final_name = translation_map.get(cert_norm, raw_name)
            final_norm = self. normalize_certname(final_name)
            
            # Zoek of taak al bestaat in de huidige todo-lijst
            mask = (todo_work["staffGID"] == staff_id) & (todo_work["CertName_norm"] == final_norm)
            
            dt = pd.to_datetime(req_row.get("ScheduledDate"), errors='coerce')
            loc = str(req_row. get("Location", "")).strip().replace("nan", "")

            if mask.any():
                idx = todo_work[mask].index[0]
                # Update bestaande open taak
                if str(todo_work.at[idx, "Status"]).lower() not in ["afgewerkt", "closed", "gesloten"]:
                    todo_work.at[idx, "Status"] = "Ingeschreven"
                    todo_work.at[idx, "Status_Detail"] = "Bevestigd in Xaurum"
                    todo_work.at[idx, "Ingeschreven_Datum"] = dt
                    todo_work.at[idx, "Ingeschreven_Locatie"] = loc
                    todo_work.at[idx, "LastUpdatedAt"] = now
                    updates += 1
            else:
                # Nieuwe taak toevoegen (alleen omdat we weten dat staff_id in my_department_gids zit)
                s_info = staff_lookup.get(staff_id)
                if s_info: 
                    new_row = {
                        "staffGID":  staff_id, 
                        "staffSAPNR":  s_info["sapnr"],
                        "MedewerkerID": staff_id, 
                        "MedewerkerNaam": s_info["name"],
                        "CostCenter":  s_info["costcenter"], 
                        "CertName": final_name, 
                        "CertName_norm": final_norm, 
                        "TaskType": "Certificaat",
                        "Status":  "Ingeschreven", 
                        "Status_Detail":  "Bevestigd in Xaurum",
                        "Nodig": 1, 
                        "Ingeschreven_Datum": dt, 
                        "Ingeschreven_Locatie": loc,
                        "CreatedAt": now, 
                        "LastUpdatedAt":  now, 
                        "CreatedBy": "sync_safe_V20"
                    }
                    todo_work = pd. concat([todo_work, pd.DataFrame([new_row])], ignore_index=True)
                    new_tasks_count += 1

        # 5. TERUGSCHRIJVEN EN OPSLAAN
        self.df["todo"] = todo_work
        
        if updates > 0 or new_tasks_count > 0:
            self.save_todo_planner() # Schrijft alleen de gefilterde lijst terug
            
        print(f"✅ Sync voltooid. {updates} updates, {new_tasks_count} nieuwe taken voor afdeling {self. active_costcenter}.")
        return updates + new_tasks_count
    
    def _build_staff_lookup(self, df):
        # Niet meer nodig in deze versie, mag leeg blijven of weg
        return {}

    def sync_failed_results_to_todo(self) -> int:
        """
        Maak/actualiseer todo-taken voor niet-geslaagde resultaten (cert_results),
        beperkt tot het huidig geladen costcenter (self.df['staff'] is al gefilterd).

        Doel: als iemand niet geslaagd is maar er nog geen taak bestaat, toch een open taak tonen.
        
        V2-FIX: CostCenter lookup volgorde aangepast om te werken na kolom-hernoemen in load_all().
        """
        import pandas as pd

        staff = self.df. get("staff", pd. DataFrame())
        todo = self.df.get("todo", pd.DataFrame())
        results = self.df. get("cert_results", pd.DataFrame())

        if staff is None or staff.empty or results is None or results. empty:
            return 0

        id_col = self.get_id_column() or "staffGID"

        # bepaal kolommen in results
        res_id_col = None
        for c in ("staffGID", "staffSAPNR", id_col):
            if c in results. columns:
                res_id_col = c
                break
        if not res_id_col:
            return 0

        res_cert_col = None
        for c in ("CertName", "Certificaat"):
            if c in results.columns:
                res_cert_col = c
                break
        if not res_cert_col:
            return 0

        res_status_col = "Status" if "Status" in results.columns else None
        if not res_status_col:
            return 0

        res_date_col = None
        for c in ("Behaald", "Exam_Date", "ExamDate", "Einde_sessie", "CompletedDate"):
            if c in results.columns:
                res_date_col = c
                break

        # normaliseer staff ids set (costcenter selectie)
        staff_ids = set(staff[id_col].astype(str).str.strip().unique()) if id_col in staff.columns else set()
        if not staff_ids: 
            return 0

        # normalize results
        df = results.copy()
        df[res_id_col] = df[res_id_col].astype(str).str.strip()
        df[res_cert_col] = df[res_cert_col].astype(str).str.strip()
        df["CertName_norm"] = df[res_cert_col].apply(self.normalize_certname)
        df[res_status_col] = df[res_status_col]. astype(str).str.strip().str.lower()
        if res_date_col:
            df[res_date_col] = pd.to_datetime(df[res_date_col], errors="coerce")

        # filter op huidig costcenter
        df = df[df[res_id_col]. isin(staff_ids)].copy()
        if df.empty:
            return 0

        failed_statuses = {"not certified", "failed", "niet geslaagd", "gefaald", "mislukt"}
        certified_statuses = {"certified", "passed", "geslaagd", "behaald", "ok"}

        # Neem per medewerker+cert de meest recente result (of laatste rij als geen datum)
        sort_cols = [res_id_col, "CertName_norm"]
        if res_date_col: 
            df = df.sort_values(sort_cols + [res_date_col], ascending=[True, True, False], kind="mergesort")
        else:
            df = df.sort_values(sort_cols, ascending=[True, True], kind="mergesort")
        df = df.drop_duplicates(subset=[res_id_col, "CertName_norm"], keep="first").copy()

        # Bepaal welke combos latest = failed
        df_failed = df[df[res_status_col]. isin(failed_statuses)].copy()
        if df_failed. empty:
            return 0

        # todo lookup (best effort)
        todo_df = todo. copy() if todo is not None else pd. DataFrame()
        if todo_df.empty:
            todo_df = pd. DataFrame(columns=["staffGID", "CertName_norm", "TaskType", "Status"])
        if "staffGID" in todo_df.columns:
            todo_df["staffGID"] = todo_df["staffGID"]. astype(str).str.strip()
        if "CertName_norm" not in todo_df. columns and "CertName" in todo_df.columns:
            todo_df["CertName_norm"] = todo_df["CertName"].astype(str).apply(self.normalize_certname)

        def _status_lc(s):
            return str(s or "").strip().lower()

        created = 0
        updated = 0
        now = pd.Timestamp.now()

        # staff lookup voor velden
        name_col_staff = None
        for c in ("FullName", "Name+Firstname", "Naam", "Employee_Name", "MedewerkerNaam"):
            if c in staff.columns:
                name_col_staff = c
                break
        
        # V2-FIX:  Zoek eerst naar CostCenter (na hernoemen in load_all), dan fallback naar originele naam
        cc_col_staff = None
        for c in ("CostCenter", "staffCOSTCENTER315"):
            if c in staff.columns:
                cc_col_staff = c
                break
        
        sap_col_staff = "staffSAPNR" if "staffSAPNR" in staff.columns else None

        staff_map = {}
        for _, r in staff.iterrows():
            sid = str(r. get(id_col, "")).strip()
            if not sid:
                continue
            staff_map[sid] = {
                "name":  str(r. get(name_col_staff, "") or "").strip() if name_col_staff else "",
                "cc": str(r. get(cc_col_staff, "") or self.active_costcenter or "").strip() if cc_col_staff else str(self.active_costcenter or "").strip(),
                "sap": str(r.get(sap_col_staff, "") or "").strip() if sap_col_staff else "",
            }

        new_rows = []

        for _, r in df_failed.iterrows():
            sid = str(r.get(res_id_col, "")).strip()
            cert_name = str(r.get(res_cert_col, "")).strip()
            cert_norm = str(r.get("CertName_norm", "")).strip()
            if not sid or not cert_norm:
                continue

            # skip als er een ingeschreven taak bestaat
            existing = todo_df[(todo_df. get("staffGID", "").astype(str) == sid) & (todo_df. get("CertName_norm", "").astype(str) == cert_norm)]
            if not existing.empty:
                # als er al een open/ingeschreven taak is, niets doen
                statuses = existing["Status"].astype(str).map(_status_lc) if "Status" in existing. columns else pd.Series([])
                if any(s in ("ingeschreven", "open", "in wachtrij", "gepland") for s in statuses. tolist()):
                    continue
                # als alleen "afgewerkt/gesloten" bestaat -> heropen
                idx0 = existing.index[0]
                todo_df.at[idx0, "Status"] = "Open"
                datum = r.get(res_date_col) if res_date_col else pd.NaT
                datum_str = ""
                if isinstance(datum, pd.Timestamp) and not pd.isna(datum):
                    datum_str = datum.strftime("%d-%m-%Y")
                todo_df.at[idx0, "Status_Detail"] = f"Niet geslaagd ({datum_str}) - herinschrijving nodig" if datum_str else "Niet geslaagd - herinschrijving nodig"
                todo_df.at[idx0, "LastUpdatedAt"] = now
                todo_df.at[idx0, "Nodig"] = True
                updated += 1
                continue

            info = staff_map. get(sid, {})
            datum = r.get(res_date_col) if res_date_col else pd.NaT
            datum_str = ""
            if isinstance(datum, pd.Timestamp) and not pd.isna(datum):
                datum_str = datum.strftime("%d-%m-%Y")

            new_rows.append(
                {
                    "staffGID":  sid,
                    "staffSAPNR": info.get("sap", ""),
                    "MedewerkerID":  sid,
                    "MedewerkerNaam": info. get("name", ""),
                    "CostCenter": info. get("cc", ""),
                    "CertName": cert_name,
                    "CertName_norm": cert_norm,
                    "TaskType": "Certificaat",
                    "Status": "Open",
                    "Status_Detail": f"Niet geslaagd ({datum_str}) - herinschrijving nodig" if datum_str else "Niet geslaagd - herinschrijving nodig",
                    "Nodig": True,
                    "Commentaar":  "",
                    "CreatedAt": now,
                    "LastUpdatedAt": now,
                    "CreatedBy": "sync_failed_results_to_todo",
                }
            )
            created += 1

        if new_rows:
            todo_df = pd. concat([todo_df, pd.DataFrame(new_rows)], ignore_index=True)

        if created > 0 or updated > 0:
            self.df["todo"] = todo_df
            print(f"   ✅ sync_failed_results_to_todo: nieuw={created}, heropend={updated}")

        return created + updated
    
    def find_training_url_for_cert(self, cert_name: str) -> str | None:
        """Zoek training URL in catalogus."""
        if self.training_catalog is None or self.training_catalog.empty:
            return None

        df = self.training_catalog.copy()
        norm = self.normalize_certname

        cert_norm = norm(cert_name)

        base_col = None
        for c in ("CertName", "raw_text", "title", "Title"):
            if c in df.columns:
                base_col = c
                break

        if not base_col:
            return None

        df["CertName_norm"] = df[base_col].astype(str).map(norm)

        url_col = None
        for c in ("url", "Url", "URL"):
            if c in df.columns:
                url_col = c
                break

        if url_col is None:
            return None

        mask = df["CertName_norm"] == cert_norm
        if mask.any():
            return str(df.loc[mask, url_col].iloc[0])

        code_pattern = r"\b([A-Za-z]{2}(?:-[A-Za-z])?-\d{3})\b"
        code = None
        m = re.search(code_pattern, str(cert_name))
        if m:
            code = m.group(1).upper()

        if not code:
            m = re.search(code_pattern, cert_norm)
            if m:
                code = m.group(1).upper()

        if code:
            for code_col in ("code", "Code", "training_id", "trainingID"):
                if code_col in df.columns:
                    mask_code = df[code_col].astype(str).str.upper().str.strip() == code
                    if mask_code.any():
                        return str(df.loc[mask_code, url_col].iloc[0])

            for text_col in ("raw_text", "title", "Title"):
                if text_col in df.columns:
                    mask_code = df[text_col].astype(str).str.upper().str.contains(code, na=False)
                    if mask_code.any():
                        return str(df.loc[mask_code, url_col].iloc[0])

        return None
    
    def normalize_legacy_statuses(self):
        """Normaliseer oude statussen."""
        todo = self.df.get("todo", pd.DataFrame())
        if todo is None or todo.empty:
            return

        changed = False

        if "Status" in todo.columns:
            todo["Status"] = todo["Status"].astype(str)
        if "Status_Detail" in todo.columns:
            todo["Status_Detail"] = todo["Status_Detail"].astype(str)

        for idx, row in todo.iterrows():
            status = str(row.get("Status", "") or "").strip()
            detail = str(row.get("Status_Detail", "") or "").strip()

            status_lower = status.lower()
            detail_lower = detail.lower()

            if status_lower == "overruled":
                if "geconfirmeerd in req" in detail_lower or "bevestigd in req" in detail_lower:
                    todo.at[idx, "Status"] = "Ingeschreven"
                    todo.at[idx, "Status_Detail"] = "Bevestigd in Xaurum"
                else:
                    todo.at[idx, "Status"] = "Open"
                    if detail_lower == "nan":
                        todo.at[idx, "Status_Detail"] = ""
                changed = True

        if changed:
            self.df["todo"] = todo
            try:
                self.save_todo()
            except Exception:
                pass

    def update_status_from_tasktype_and_xaurum(self):
        """Update status vanuit Xaurum."""
        todo = self.df.get("todo", pd.DataFrame())
        req = self.df.get("training_req", pd.DataFrame())

        if todo is None or todo.empty:
            return

        if "Status_Detail" not in todo.columns:
            todo["Status_Detail"] = ""

        if req is not None and not req.empty:
            req = req.copy()
            if "staffGID" in req.columns:
                req["staffGID"] = req["staffGID"].astype(str).str.strip()
            if "CertName" in req.columns:
                req["CertName"] = req["CertName"].astype(str).str.strip()
            if "RequestStatus" in req.columns:
                req["RequestStatus"] = req["RequestStatus"].astype(str)
            if "ScheduledDate" in req.columns:
                req["ScheduledDateParsed"] = pd.to_datetime(
                    req["ScheduledDate"], errors="coerce"
                )
        else:
            req = pd.DataFrame()

        # 🔧 FIX: self. toevoegen!
        if "CertName" in todo.columns:
            todo["CertName_norm"] = todo["CertName"].astype(str).apply(self.normalize_certname)
        else:
            todo["CertName_norm"] = ""

        if not req.empty and "CertName" in req.columns:
            # 🔧 FIX: self. toevoegen!
            req["CertName_norm"] = req["CertName"].astype(str).apply(self.normalize_certname)

        today = pd.Timestamp.today().normalize()
        changed = False

        for idx, row in todo.iterrows():
            status = str(row.get("Status", "") or "").strip().lower()
            tasktype = str(row.get("TaskType", "") or "").strip()
            staff_id = str(row.get("staffGID", "") or "").strip()
            cert_name = str(row.get("CertName", "") or "").strip()
            cert_name_norm = str(row.get("CertName_norm", "") or "").strip()

            if status == "open":
                if not req.empty and staff_id and cert_name:
                    base_mask = (req["staffGID"] == staff_id)

                    if "CertName_norm" in req.columns and cert_name_norm:
                        mask_cert = (req["CertName_norm"] == cert_name_norm)
                    else:
                        mask_cert = (req["CertName"] == cert_name)

                    if not mask_cert.any() and cert_name_norm:
                        m = re.match(r"([A-Za-z]{2}-[A-Za-z]-\d{3})", cert_name_norm)
                        if m and "CertName_norm" in req.columns:
                            code = m.group(1)
                            mask_cert = req["CertName_norm"].astype(str).str.startswith(code)

                    mask = base_mask & mask_cert
                    match_req = req[mask]

                    if not match_req.empty:
                        cand = match_req.copy()
                        cand = cand[cand["ScheduledDateParsed"].notna()]
                        if not cand.empty:
                            future = cand[cand["ScheduledDateParsed"] >= today]
                            if not future.empty:
                                chosen = future.sort_values("ScheduledDateParsed").iloc[0]
                            else:
                                chosen = cand.sort_values("ScheduledDateParsed").iloc[0]

                            sched_dt = chosen["ScheduledDateParsed"]
                            loc = str(chosen.get("Location", "") or "").strip()

                            todo.at[idx, "Status"] = "Ingeschreven"
                            todo.at[idx, "Status_Detail"] = "Bevestigd in Xaurum (auto)"
                            todo.at[idx, "Ingeschreven_Datum"] = sched_dt
                            todo.at[idx, "Ingeschreven_Locatie"] = loc
                            changed = True
                            continue

                detail = str(todo.at[idx, "Status_Detail"] or "").strip()
                if not detail:
                    tt = tasktype.lower()
                    if "vervalt binnen 6 maanden" in tt:
                        todo.at[idx, "Status_Detail"] = "Vervalt binnen 6 maanden"
                        changed = True
                    elif tt == "auto":
                        todo.at[idx, "Status_Detail"] = "Vervallen"
                        changed = True
                    elif "nieuwe vereiste opleiding" in tt:
                        todo.at[idx, "Status_Detail"] = "Geen certificaat / nieuwe vereiste opleiding"
                        changed = True

        if changed:
            self.df["todo"] = todo
            try:
                self.save_todo()
            except Exception:
                pass

    def get_upcoming_trainings(self, days:  int = 21) -> pd.DataFrame:
        """
        Haal aankomende trainingen op. 
        
        V2-FIX: CostCenter kolomnaam aangepast om te werken na kolom-hernoemen in load_all().
        """
        req = self.df.get("training_req", pd.DataFrame())
        staff = self.df. get("staff", pd.DataFrame())
        if req is None or req.empty:
            return pd.DataFrame()

        df = req.copy()
        id_col = self.get_id_column()
        if id_col is None:
            id_col = "staffGID"

        if "staffGID" in df. columns:
            df["staffGID"] = df["staffGID"].astype(str).str.strip()
        elif id_col in df. columns:
            df["staffGID"] = df[id_col].astype(str).str.strip()
        else:
            return pd. DataFrame()

        date_col = None
        if "ScheduledDateParsed" in df.columns:
            date_col = "ScheduledDateParsed"
        elif "ScheduledDate" in df.columns:
            df["ScheduledDateParsed"] = pd. to_datetime(
                df["ScheduledDate"], errors="coerce"
            )
            date_col = "ScheduledDateParsed"

        if not date_col: 
            return pd.DataFrame()

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        today = pd.Timestamp.today().normalize()
        upper = today + pd.Timedelta(days=days)

        mask = df[date_col].notna() & (df[date_col] >= today) & (df[date_col] <= upper)

        if "RequestStatus" in df.columns:
            statuses = df["RequestStatus"].astype(str).dropna().str.lower().unique()
            if any("goedgekeurd" in s for s in statuses):
                mask &= df["RequestStatus"].astype(str).str.contains(
                    "goedgekeurd", case=False, na=False
                )

        df = df[mask].copy()
        if df.empty:
            return df

        if staff is not None and not staff.empty:
            st = staff.copy()
            if "staffGID" in st.columns:
                st["staffGID"] = st["staffGID"].astype(str).str.strip()
            elif id_col in st.columns:
                st["staffGID"] = st[id_col].astype(str).str.strip()
            else:
                st["staffGID"] = ""

            name_col = None
            for c in ["FullName", "Name+Firstname", "Naam", "Employee_Name"]: 
                if c in st.columns:
                    name_col = c
                    break
            if not name_col:
                name_col = st. columns[0]

            # V2-FIX: Zoek CostCenter in beide mogelijke kolomnamen
            cc_col = None
            for c in ("CostCenter", "staffCOSTCENTER315"):
                if c in st.columns:
                    cc_col = c
                    break

            cols = ["staffGID", name_col]
            if cc_col:
                cols.append(cc_col)

            st_subset = st[cols].drop_duplicates("staffGID")
            st_subset = st_subset.rename(columns={name_col: "MedewerkerNaam"})
            
            # V2-FIX: Hernoem de CostCenter kolom naar een consistente naam voor filtering
            if cc_col and cc_col != "CostCenter": 
                st_subset = st_subset.rename(columns={cc_col: "CostCenter"})

            df = df.merge(st_subset, on="staffGID", how="left")

        # V2-FIX: Filter nu op "CostCenter" (consistente naam na merge)
        if self.active_costcenter and "CostCenter" in df.columns:
            df = df[
                df["CostCenter"].astype(str).str.strip()
                == str(self.active_costcenter).strip()
            ].copy()

        loc_col = None
        for c in ["Location", "Locatie", "Plaats"]:
            if c in df.columns:
                loc_col = c
                break

        df["LocatieAlert"] = ""
        if loc_col: 
            df["LocatieAlert"] = df[loc_col].astype(str).fillna("")

        if "CertName" in df.columns:
            df["CertName"] = df["CertName"].astype(str).str.strip()

        df = df.sort_values(
            by=[date_col, "MedewerkerNaam", "CertName"],
            ascending=[True, True, True],
            na_position="last",
        )

        return df
    
    def merge_todo_with_config(self):
        """
        Update de 'Nodig' status in todo op basis van de HUIDIGE config.
        🔧 GEFIXTE VERSIE: 
          - Gebruikt 'config_cert' (dezelfde bron als sync) om mismatches te voorkomen.
          - Verwijdert GEEN taken hardhandig, maar update enkel de kolom 'Nodig'.
        """
        todo = self.df.get("todo")
        
        # 1. Gebruik dezelfde config-bron als de sync (config_cert heeft prioriteit)
        cfg = self.df.get("config_cert")
        if cfg is None or cfg.empty:
            cfg = self.df.get("config")

        if todo is None or cfg is None or todo.empty or cfg.empty:
            return

        # Werk op kopieën om warnings te voorkomen
        todo = todo.copy()
        cfg = cfg.copy()

        # 2. Helpers (lokaal of via self)
        norm = getattr(self, "normalize_certname", lambda x: str(x).strip().lower())
        is_true = lambda x: str(x).lower() in ['true', '1', 'ja', 'yes', 't']

        # 3. Zorg dat ID kolommen matchen
        id_col = self.get_id_column() # Haal de ID kolom op (bv staffGID)
        
        if id_col and id_col in todo.columns:
            todo[id_col] = todo[id_col].astype(str).str.strip()
        if id_col and id_col in cfg.columns:
            cfg[id_col] = cfg[id_col].astype(str).str.strip()

        # 4. Zorg dat CertName_norm bestaat voor matching
        if "CertName_norm" not in todo.columns and "CertName" in todo.columns:
            todo["CertName_norm"] = todo["CertName"].astype(str).apply(norm)
        
        if "CertName_norm" not in cfg.columns and "CertName" in cfg.columns:
            cfg["CertName_norm"] = cfg["CertName"].astype(str).apply(norm)

        # 5. Check of we kunnen mergen
        if id_col not in todo.columns or "CertName_norm" not in todo.columns:
            return # Kan niet matchen
        
        if id_col not in cfg.columns or "CertName_norm" not in cfg.columns:
            return # Config mist kolommen

        # Bewaar bestaande Nodig (belangrijk voor ingeschreven taken die niet in config staan)
        # We gebruiken een row-id om de oorspronkelijke waarden na de merge terug te kunnen zetten.
        todo["_RowId"] = range(len(todo))
        if "Nodig" in todo.columns:
            todo["_Nodig_prev"] = todo["Nodig"]
            todo.drop(columns=["Nodig"], inplace=True)

        # 6. Merge: rename config kolom naar vaste naam zodat 'Nodig_cfg' altijd bestaat
        cfg_for_merge = cfg[[id_col, "CertName_norm", "Nodig"]].copy()
        cfg_for_merge.rename(columns={"Nodig": "Nodig_cfg"}, inplace=True)

        merged = todo.merge(
            cfg_for_merge,
            on=[id_col, "CertName_norm"],
            how="left",
        )

        # 7. Update de 'Nodig' waarde
        # - Als config matcht: neem Nodig_cfg
        # - Als geen config match: behoud vorige Nodig (indien aanwezig), anders False
        # - Bescherm ingeschreven taken: die moeten zichtbaar blijven (Nodig=True),
        #   ook als ze (nog) niet in config staan (zie "Toekomstige inschrijvingen" tab).
        merged["Nodig"] = merged["Nodig_cfg"].apply(lambda v: is_true(v) if not pd.isna(v) else False)

        if "_Nodig_prev" in merged.columns:
            missing_cfg = merged["Nodig_cfg"].isna()
            merged.loc[missing_cfg, "Nodig"] = merged.loc[missing_cfg, "_Nodig_prev"].apply(is_true)

        # Forceer Nodig=True voor ingeschreven/planned taken
        try:
            status_series = merged.get("Status", "").astype(str).str.lower()
        except Exception:
            status_series = pd.Series([""] * len(merged))

        planned = status_series.eq("ingeschreven")
        if "Ingeschreven_Datum" in merged.columns:
            planned = planned | pd.to_datetime(merged["Ingeschreven_Datum"], errors="coerce").notna()
        if "Status_Detail" in merged.columns:
            planned = planned | merged["Status_Detail"].astype(str).str.lower().str.contains("ingeschreven", na=False)
        merged.loc[planned, "Nodig"] = True

        # Bescherm "niet geslaagd" taken (herinschrijving nodig) — ook als ze niet in config staan
        try:
            failed_mask = pd.Series([False] * len(merged))
            if "Status_Detail" in merged.columns:
                failed_mask = failed_mask | merged["Status_Detail"].astype(str).str.lower().str.contains("niet geslaagd", na=False)
                failed_mask = failed_mask | merged["Status_Detail"].astype(str).str.lower().str.contains("herinschrijving", na=False)
            if "CreatedBy" in merged.columns:
                failed_mask = failed_mask | (merged["CreatedBy"].astype(str) == "sync_failed_results_to_todo")
            merged.loc[failed_mask, "Nodig"] = True
        except Exception:
            pass

        # Opruimen hulpkolommen
        merged.drop(columns=["Nodig_cfg"], inplace=True, errors="ignore")
        merged.drop(columns=["_RowId", "_Nodig_prev"], inplace=True, errors="ignore")

        # 8. Opslaan terug in dataframe (zonder rijen weg te gooien!)
        self.df["todo"] = merged
        
        # Debug output
        print(f"   ℹ️ merge_todo_with_config: {len(merged)} taken bijgewerkt (Nodig status synced).")

    
    def standardize_config_certnames(self) -> None:
        """
        UI-compat:
        Vertaalt Franse namen in de config naar Nederlandse standaard (via SQL dictionary)
        en herberekent daarna de genormaliseerde naam.
        """
        # 1. Haal de dataframe op
        cfg = self.df.get("config_cert")
        if cfg is None or cfg.empty:
            cfg = self.df.get("config")
        
        if cfg is None or cfg.empty:
            return

        cfg = cfg.copy()
        if "CertName" not in cfg.columns:
            self.df["config_cert"] = cfg
            return

        # 2. DE VERTAALSLAG (Hier zat het probleem)
        # We gebruiken nu self.translation_dict i.p.v. de lege dummy functie
        def _translate_cert(x):
            s = str(x).strip()
            # Als we een vertaling hebben in het geheugen, gebruik die!
            if self.translation_dict and s in self.translation_dict:
                return self.translation_dict[s]
            return s

        # Pas vertaling toe: 'Secouriste' wordt 'Hulpverlener'
        cfg["CertName"] = cfg["CertName"].apply(_translate_cert)
        
        # 3. DE NORMALISATIESLAG
        # Maak de technische zoeksleutel: 'Hulpverlener' wordt 'hulpverlener'
        try:
            cfg["CertName_norm"] = cfg["CertName"].apply(self.normalize_certname)
        except Exception:
            # Fallback als er iets misgaat
            if "CertName_norm" not in cfg.columns:
                cfg["CertName_norm"] = cfg["CertName"].astype(str).apply(lambda s: str(s).strip().lower())

        # 4. TERUGSCHRIJVEN
        self.df["config_cert"] = cfg
        self.df["config"] = cfg
        

    # ══════════════════════════════════════════════════════════════
    # BACKWARDS COMPATIBILITY HELPERS (UI verwacht deze methodes)
    # ══════════════════════════════════════════════════════════════
    def renormalize_all_certnames(self) -> None:
        """
        Backwards compat:
        Herbouwt *_norm kolommen op basis van de huidige normalizer.
        Wordt (in sommige UI versies) na load_all() aangeroepen.
        """
        norm = getattr(self, "normalize_certname", lambda x: str(x).strip().lower())

        def _safe_norm_series(series: pd.Series) -> pd.Series:
            try:
                return series.astype(str).apply(norm)
            except Exception:
                return series.apply(lambda x: norm(x) if x is not None else "")

        # Cert-gerelateerde DF's
        for key in ("certificates", "cert_results", "training_req", "config_cert", "todo"):
            df = self.df.get(key)
            if df is None or df.empty:
                continue
            if "CertName" in df.columns:
                df["CertName_norm"] = _safe_norm_series(df["CertName"])
            self.df[key] = df

        # Competence-gerelateerde DF's
        for key in ("competences", "competence_config", "config_comp"):
            df = self.df.get(key)
            if df is None or df.empty:
                continue
            if "Competence" in df.columns:
                df["Competence_norm"] = _safe_norm_series(df["Competence"])
            self.df[key] = df

        # Search sets (voor UI-zoekfunctie)
        try:
            self.all_cert_names = set()
            self.all_competence_names = set()
            if hasattr(self, "master_cert_all") and isinstance(self.master_cert_all, pd.DataFrame) and not self.master_cert_all.empty and "CertName" in self.master_cert_all.columns:
                self.all_cert_names = set(self.master_cert_all["CertName"].dropna().astype(str).unique())
            if hasattr(self, "master_comp_all") and isinstance(self.master_comp_all, pd.DataFrame) and not self.master_comp_all.empty and "Competence" in self.master_comp_all.columns:
                self.all_competence_names = set(self.master_comp_all["Competence"].dropna().astype(str).unique())
        except Exception:
            pass

        # Display map cache invalidatie
        self._cert_display_map = {}
        self._cert_display_map_built = False

    def _build_cert_display_map(self) -> Dict[str, str]:
        """
        Bouwt een mapping van genormaliseerde certificaatnaam -> originele (leesbare) naam.
        Gebruikt alle beschikbare bronnen (master/config/certificates/training catalog).
        """
        if getattr(self, "_cert_display_map_built", False) and isinstance(getattr(self, "_cert_display_map", None), dict) and self._cert_display_map:
            return self._cert_display_map

        norm = getattr(self, "normalize_certname", lambda x: str(x).strip().lower())
        display_map: Dict[str, str] = {}

        def _ingest_names(values) -> None:
            try:
                for v in values:
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        continue
                    s = str(v).strip()
                    if not s or s.lower() == "nan":
                        continue
                    try:
                        k = str(norm(s))
                    except Exception:
                        k = ""
                    if k and k not in display_map:
                        display_map[k] = s
            except Exception:
                return

        # 1) Master certs (beste bron)
        try:
            df_master = getattr(self, "master_cert_all", None)
            if isinstance(df_master, pd.DataFrame) and not df_master.empty and "CertName" in df_master.columns:
                _ingest_names(df_master["CertName"].dropna().astype(str).tolist())
        except Exception:
            pass

        # 2) In-memory DF's
        for key in ("master_cert", "config_cert", "certificates", "training_req", "todo"):
            try:
                df = self.df.get(key)
                if isinstance(df, pd.DataFrame) and not df.empty and "CertName" in df.columns:
                    _ingest_names(df["CertName"].dropna().astype(str).tolist())
            except Exception:
                continue

        # 3) Training catalog (fallback, kolom kan title/Title/raw_text zijn)
        try:
            cat_df = getattr(self, "training_catalog", None)
            if cat_df is None or (isinstance(cat_df, pd.DataFrame) and cat_df.empty):
                cat_df = self.df.get("training_catalog")
            if isinstance(cat_df, pd.DataFrame) and not cat_df.empty:
                title_col = None
                for c in ("title", "Title", "raw_text"):
                    if c in cat_df.columns:
                        title_col = c
                        break
                if title_col:
                    _ingest_names(cat_df[title_col].dropna().astype(str).tolist())
        except Exception:
            pass

        self._cert_display_map = display_map
        self._cert_display_map_built = True
        return display_map

    def get_display_certname(self, certname: str, employee_language: Optional[str] = None) -> str:
        """
        Backwards compat:
        Geeft een leesbare naam terug voor een (mogelijk al-genormaliseerde) cert key.
        employee_language wordt genegeerd (geen vertaalbron beschikbaar), maar blijft in signature
        zodat oudere UI code niet crasht.
        """
        if certname is None:
            return ""
        raw = str(certname).strip()
        if not raw or raw.lower() == "nan":
            return ""

        # Als het al leesbaar lijkt, return as-is
        if " " in raw or " - " in raw or len(raw) > 40:
            return raw

        display_map = self._build_cert_display_map()
        # 1) direct match (sommige keys zijn al norm)
        if raw in display_map:
            return display_map[raw]

        # 2) normalized match
        try:
            k = str(self.normalize_certname(raw))
            if k in display_map:
                return display_map[k]
        except Exception:
            pass

        return raw

    def normalize_certname_to_standard(self, name: str) -> str:
        """
        V19: DE MOOIE WASSTRAAT (Leesbare tekst maker)
        Zorgt dat 'EA-E-294 HS' opgeslagen wordt als 'EA-E-294 - BA5 Hoogspanning'
        """
        import re
        if not name: return ""
        s = str(name).strip()

        # 1. Reverse lookup (Behoud ID-vertaling uit je oude versie)
        try:
            if " " not in s and "-" not in s and len(s) >= 5:
                if hasattr(self, "get_display_certname"):
                    s_disp = self.get_display_certname(s)
                    if s_disp and s_disp != s: s = s_disp
        except: pass

        # 2. Afkortingen voluit schrijven voor de database-tekst
        s = re.sub(r'\bHS\b', 'Hoogspanning', s, flags=re.IGNORECASE)
        s = re.sub(r'\bLS\b', 'Laagspanning', s, flags=re.IGNORECASE)

        # 3. Vertalen via de 41 SQL regels
        if hasattr(self, "translation_dict") and self.translation_dict:
            if s in self.translation_dict:
                s = self.translation_dict[s]
        
        return s
    
    def create_tasks_for_expiring_certificates(self) -> int:
        """
        Backwards compat:
        In oudere UI's aangeroepen na load_all() om (renewal) taken te creëren.
        In deze codebase zit die logica in sync_cert_tasks(); we roepen die veilig aan.
        Returns: aantal nieuw toegevoegde todo-rijen (best effort).
        """
        before = 0
        try:
            before = len(self.df.get("todo", pd.DataFrame()))
        except Exception:
            before = 0

        try:
            self.sync_cert_tasks()
        except Exception as e:
            # Houd het non-fatal; UI kan verder laden.
            try:
                self.errors.append(f"create_tasks_for_expiring_certificates faalde: {e}")
            except Exception:
                pass
            return 0

        after = 0
        try:
            after = len(self.df.get("todo", pd.DataFrame()))
        except Exception:
            after = before

        created = max(0, after - before)

        # Persist als we effectief iets toegevoegd hebben
        if created > 0:
            try:
                if getattr(self, "USE_SQL_FOR_TODO", False):
                    self.save_todo()
            except Exception:
                pass

        return created
    
  
    def sync_todo_with_config(self):
        """
        Synchroniseert todo met config en creëert automatisch taken. 
        
        🔧 GEFIXTE VERSIE V10 (Safe Port of V8):
        - BEHOUDT:  De 'Protected Status' logica uit jouw V8 (overschrijft geen ingeschreven taken).
        - BEHOUDT: De 'Gefaald/Herkansing' check uit jouw V8. 
        - BEHOUDT:  Negeert TaskType bij het checken van bestaande taken. 
        - FIX: Gebruikt 'self.normalize_certname' (geen globals/crashes meer).
        - V10-FIX: CostCenter lookup aangepast om te werken na kolom-hernoemen in load_all().
        """
        # Helper:  Gebruik de interne functie (veilig)
        norm = self.normalize_certname
        
        # 1. DATA LADEN
        cfg = self.df. get("config_cert")
        if cfg is None or cfg.empty:
             cfg = self.df. get("config", pd.DataFrame()) # Fallback

        todo = self.df. get("todo", pd.DataFrame())
        certs = self.df.get("certificates", pd.DataFrame())
        results = self.df. get("cert_results", pd. DataFrame())
        staff = self.df. get("staff", pd.DataFrame())

        if cfg is None or cfg.empty:
            print("   ⚠️ sync_todo_with_config: Geen config data")
            self.df["todo"] = todo
            return

        # Zoek ID kolom
        id_cfg = next((c for c in ["staffGID", "staffSAPNR", "staffSA"] if c in cfg. columns), None)
        if id_cfg is None:
            print("   ⚠️ sync_todo_with_config: Geen ID kolom in config")
            self.df["todo"] = todo
            return

        print(f"\n   🔄 sync_todo_with_config() V10 [SAFE PORT]")
        print(f"      Config: {len(cfg)} | Todo: {len(todo)}")

        # 2. NORMALISATIE (ID kolommen)
        # Zorg dat alle ID kolommen strings zijn voor correcte matching
        for df_tmp, name in [(cfg, "cfg"), (todo, "todo"), (certs, "certs"), (results, "results"), (staff, "staff")]:
            if not df_tmp.empty and id_cfg in df_tmp.columns:
                df_tmp[id_cfg] = df_tmp[id_cfg].astype(str).str.strip()

        # 3. NORMALISATIE (CertName) - DE FIX
        # We gebruiken hier self.normalize_certname in plaats van de onveilige globals
        if "CertName" in cfg. columns:
            cfg["CertName_norm"] = cfg["CertName"].apply(norm)
        
        if not todo.empty and "CertName" in todo.columns:
            todo["CertName_norm"] = todo["CertName"].astype(str).apply(norm)
            
        # Zorg dat results en certs ook een norm kolom hebben voor de lookups
        if not certs.empty and "CertName" in certs.columns and "CertName_norm" not in certs.columns:
            certs["CertName_norm"] = certs["CertName"].astype(str).apply(norm)
        
        if not results.empty and "CertName" in results.columns and "CertName_norm" not in results.columns:
             # Probeer CertName, anders Certificaat
            col = "CertName" if "CertName" in results.columns else "Certificaat"
            if col in results.columns:
                results["CertName_norm"] = results[col].astype(str).apply(norm)

        # 4. FILTER OP NODIG = TRUE
        if "Nodig" in cfg.columns:
            def is_true(x): return str(x).lower() in ['true', '1', 'ja', 'yes', 't']
            cfg_needed = cfg[cfg["Nodig"]. apply(is_true)].copy()
        else:
            cfg_needed = cfg. copy()

        if cfg_needed.empty:
            self.df["todo"] = todo
            return

        cfg_needed = cfg_needed.drop_duplicates(subset=[id_cfg, "CertName_norm"])

        # 5. BOUW EXISTING TASKS MAP (CRUCIAAL:  JOUW V8 LOGICA)
        # We negeren TaskType, we willen weten of ERGGENS al een taak is voor dit certificaat
        existing_task_map = {}
        
        if not todo.empty and id_cfg in todo. columns and "CertName_norm" in todo.columns:
            for _, row in todo. iterrows():
                sid = str(row[id_cfg]).strip()
                cn = str(row. get("CertName_norm", "")).strip()
                status = str(row. get("Status", "")).strip().lower()
                
                key = (sid, cn)
                if key not in existing_task_map:
                    existing_task_map[key] = []
                existing_task_map[key].append(status)

        # 6. CERT LOOKUP (Expiry info ophalen uit 2 bronnen)
        cert_lookup = {}
        
        # 6a.  Uit certificates
        certs_expiry_col = next((c for c in ["Expiry_Date", "ExpiryDate", "Geldig_Tot"] if c in certs.columns), None)
        if not certs.empty and "CertName_norm" in certs.columns: 
            for _, row in certs.iterrows():
                k = (str(row. get(id_cfg, "")).strip(), str(row.get("CertName_norm", "")).strip())
                exp = None
                if certs_expiry_col:
                    try:  exp = pd.to_datetime(row[certs_expiry_col])
                    except: pass
                cert_lookup[k] = {"source": "certificates", "expiry": exp, "behaald": None}
                
        # 6b. Uit cert_results
        results_expiry_col = next((c for c in ["Geldig_Tot", "ExpiryDate", "ValidUntil"] if c in results.columns), None)
        results_date_col = next((c for c in ["Behaald", "Exam_Date", "CompletedDate", "Behaald_Datum"] if c in results.columns), None)
        
        if not results.empty and "CertName_norm" in results.columns:
            # Check of kolom Status bestaat, zo ja filter op passed
            mask = slice(None) # Alles selecteren als default
            if "Status" in results.columns:
                mask = results["Status"].astype(str).str.lower().isin(["certified", "passed", "geslaagd", "behaald"])
            
            for _, row in results[mask].iterrows():
                k = (str(row.get(id_cfg, "")).strip(), str(row.get("CertName_norm", "")).strip())
                exp = None
                if results_expiry_col: 
                    try: exp = pd.to_datetime(row[results_expiry_col])
                    except:  pass
                beh = None
                if results_date_col:
                    try:  beh = pd. to_datetime(row[results_date_col])
                    except: pass
                
                # Update als deze recenter/beter is
                if k not in cert_lookup or (exp and (cert_lookup[k]["expiry"] is None or exp > cert_lookup[k]["expiry"])):
                    cert_lookup[k] = {"source": "cert_results", "expiry": exp, "behaald": beh}

        new_rows = []
        now = pd.Timestamp.now()
        today = pd.Timestamp.today().normalize()
        threshold_days = 180 
        
        # V10-FIX:  Bepaal welke CostCenter kolom we moeten gebruiken
        cc_col = None
        for c in ("CostCenter", "staffCOSTCENTER315"):
            if c in staff.columns:
                cc_col = c
                break
        
        # 7. LOOP DOOR CONFIG
        for _, row in cfg_needed.iterrows():
            staff_id = str(row[id_cfg]).strip()
            cert_name_norm = str(row. get("CertName_norm", "")).strip()
            cert_name_orig = str(row. get("CertName", "")).strip()

            if not staff_id or not cert_name_norm:  continue

            # Check actieve medewerker + Costcenter filter
            if not staff.empty:
                staff_rows = staff[staff[id_cfg] == staff_id]
                if staff_rows.empty: continue
                # V10-FIX: Gebruik de gevonden cc_col in plaats van hardcoded naam
                if hasattr(self, 'active_costcenter') and self.active_costcenter and cc_col:
                    staff_cc = str(staff_rows[cc_col]. iloc[0]).strip()
                    if staff_cc != str(self.active_costcenter).strip(): continue

            # 🛑 CRUCIALE FIX UIT JOUW V8: PROTECTED STATUS CHECK
            key_simple = (staff_id, cert_name_norm)
            
            if key_simple in existing_task_map:
                statuses = existing_task_map[key_simple]
                
                # Lijst van statussen die we NIET mogen overschrijven (uit jouw code)
                protected_states = ["ingeschreven", "gepland", "in wachtrij", "on hold", "afgewerkt", "recent behaald"]
                
                # Als een van deze statussen voorkomt -> Skip
                if any(any(p in s for p in protected_states) for s in statuses):
                    continue
                
                # Als er al een 'open' taak is -> Skip
                if any("open" in s for s in statuses):
                    continue

            # ...  Vanaf hier is het een NIEUWE taak ... 
            
            needs_task = True
            reason = "Nieuw certificaat nodig"
            expiry_date = None
            days_until = None

            # Check of het behaald is via de lookup
            if key_simple in cert_lookup:
                d = cert_lookup[key_simple]
                exp = d["expiry"]
                if exp and pd.notna(exp):
                    days_until = (pd.to_datetime(exp) - today).days
                    if days_until < 0:
                        reason = f"Certificaat verlopen ({d. get('source')})"
                    elif days_until <= threshold_days:
                        reason = f"Verloopt over {days_until} dagen"
                        expiry_date = exp
                    else: 
                        needs_task = False # Nog lang geldig
                elif d.get("behaald"):
                    needs_task = False # Behaald zonder verloopdatum
                else:
                    needs_task = False 

            # 🛑 CRUCIALE CHECK UIT JOUW V8: FAILED RESULTS
            # Als hij niet nodig lijkt (omdat er geen data is), check of hij misschien GEFAALD is
            if not needs_task and not results.empty:
                results_id_col = id_cfg if id_cfg in results. columns else "staffGID"
                if "CertName_norm" in results. columns:
                    mask = ((results[results_id_col]. astype(str).str.strip() == staff_id) &
                            (results["CertName_norm"].astype(str).str.strip() == cert_name_norm))
                            
                    if mask. any():
                        sub = results[mask].copy()
                        # Sorteer op datum
                        sort_col = next((c for c in ["Behaald", "Exam_Date", "CompletedDate"] if c in sub.columns), None)
                        if sort_col: 
                            sub[sort_col] = pd.to_datetime(sub[sort_col], errors="coerce")
                            sub = sub.sort_values(sort_col, ascending=False)
                        
                        # Check status van de laatste poging
                        last_status = str(sub.iloc[0].get("Status", "")).strip().lower()
                        if last_status in ["not certified", "failed", "niet geslaagd", "gefaald", "gezakt"]:  
                            reason = "Niet geslaagd - herinschrijving nodig"
                            needs_task = True

            if not needs_task: 
                continue

            # Maak nieuwe rij
            medewerker = ""
            sapnr = ""
            cc = ""
            if not staff.empty:
                s_row = staff[staff[id_cfg] == staff_id]
                if not s_row.empty:
                    medewerker = str(s_row.iloc[0].get("FullName", "") or s_row. iloc[0].get("MedewerkerNaam", "")).strip()
                    sapnr = str(s_row. iloc[0].get("staffSAPNR", "")).strip()
                    # V10-FIX: Gebruik cc_col of fallback naar active_costcenter
                    cc = str(s_row.iloc[0].get(cc_col, "") if cc_col else "").strip() or str(self.active_costcenter or "").strip()
            
            # Gebruik de interne USERNAME variabele (veilig)
            creator = getattr(self, "USERNAME", "System")

            new_row = {
                id_cfg:  staff_id,
                "staffGID": staff_id,
                "staffSAPNR": sapnr,
                "MedewerkerNaam": medewerker,
                "CostCenter": cc,
                "CertName":  cert_name_orig,
                "CertName_norm":  cert_name_norm,
                "TaskType": "Certificaat",
                "Status": "Open",
                "Status_Detail": reason,
                "Nodig": True,
                "Strategisch":  row.get("Strategisch", False),
                "Commentaar": row.get("Commentaar", ""),
                "CreatedAt": now,
                "LastUpdatedAt": now,
                "CreatedBy": creator,
                "ExpiryDate": expiry_date,
                "DaysUntilExpiry": days_until
            }
            new_rows.append(new_row)
            
            # Voeg toe aan map om dubbele in deze loop te voorkomen
            if key_simple not in existing_task_map:  existing_task_map[key_simple] = []
            existing_task_map[key_simple].append("open")

        # 8. SAMENVOEGEN EN OPSLAAN
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            # Voorkom warnings met pd.concat
            if not new_df.empty:
                if not todo.empty:
                    new_df = new_df.dropna(axis=1, how='all')
                    
                self. df["todo"] = pd.concat([todo, new_df], ignore_index=True)
            print(f"      ✅ {len(new_rows)} nieuwe taken toegevoegd.")
        else:
            print("      ℹ️ Geen nieuwe taken nodig.")
    
    def apply_overrule_with_zweef(self):
        """
        Apply overrule logic met zweef detection.
        """
        if "todo" not in self.df:
            return

        todo = self.df["todo"]
        if todo.empty:
            return

        req = self.df.get("training_req", pd.DataFrame())
        results = self.df.get("cert_results", pd.DataFrame())

        if "staffGID" in todo.columns:
            todo["staffGID"] = todo["staffGID"].astype(str).str.strip()

        if not req.empty:
            req = req.copy()
            id_col_req = "staffGID" if "staffGID" in req.columns else self.get_id_column()
            if id_col_req and id_col_req in req.columns:
                req["staffGID"] = req[id_col_req].astype(str).str.strip()

        if not results.empty:
            results = results.copy()
            id_col_res = "staffGID" if "staffGID" in results.columns else self.get_id_column()
            if id_col_res and id_col_res in results.columns:
                results["staffGID"] = results[id_col_res].astype(str).str.strip()
            if "CertName" in results.columns:
                results["CertName"] = results["CertName"].astype(str).str.strip()

        sched_col = None
        loc_col = None
        if not req.empty:
            for cand in ["ScheduledDate", "Planned_Date", "PlannedDate"]:
                if cand in req.columns:
                    sched_col = cand
                    break
            for cand in ["Location", "Locatie", "Plaats"]:
                if cand in req.columns:
                    loc_col = cand
                    break

        res_date_col = None
        if not results.empty:
            for cand in ["Einde_sessie", "Exam_Date", "ExamDate", "Date"]:
                if cand in results.columns:
                    res_date_col = cand
                    break

        for idx, row in todo.iterrows():
            if str(row.get("Status", "")).strip() != "Ingeschreven":
                continue

            staff_id = str(row.get("staffGID", "")).strip()
            if not staff_id:
                continue

            cert_name = str(row.get("CertName", "")).strip()
            inschrijf_datum = row.get("Ingeschreven_Datum", pd.NaT)
            locatie = str(row.get("Ingeschreven_Locatie", "") or "").strip()
            commentaar = str(row.get("Commentaar", "") or "").strip()

            match_req = pd.DataFrame()
            if not req.empty and sched_col and "staffGID" in req.columns:
                mask = req["staffGID"] == staff_id

                if not pd.isna(inschrijf_datum):
                    sched_series = pd.to_datetime(req[sched_col], errors="coerce")
                    delta_days = (sched_series - pd.to_datetime(inschrijf_datum)).dt.days.abs()
                    # Vergelijk de geplande datum met de ingeschreven datum binnen een ruimere marge.
                    # Voorheen 3 dagen, nu 10 dagen om meer realistische planningswijzigingen op te vangen.
                    mask &= delta_days <= 10

                if loc_col and locatie:
                    mask &= req[loc_col].astype(str).str.contains(locatie, case=False, na=False)

                match_req = req[mask]

            if not match_req.empty:
                todo.at[idx, "Status"] = "Ingeschreven"
                todo.at[idx, "Status_Detail"] = "Bevestigd in Xaurum"

                base_comment = str(commentaar or "").strip()

                try:
                    conf_date = pd.to_datetime(match_req[sched_col].iloc[0]).date()
                    date_str = conf_date.isoformat()
                    extra_text = f"Bevestigd in Xaurum op {date_str}."
                except Exception:
                    extra_text = "Bevestigd in Xaurum (planning)."
                    date_str = None

                if date_str is not None:
                    # 🔧 FIX: Geen spatie tussen ? en :
                    pattern = rf"(?:\s*Bevestigd in Xaurum op\s+{re.escape(date_str)}\.)+"
                    base_comment = re.sub(pattern, "", base_comment).strip()

                if extra_text not in base_comment:
                    if base_comment:
                        base_comment = base_comment.rstrip()
                        if not base_comment.endswith((".", "!", "?")):
                            base_comment += "."
                        base_comment += " "
                    base_comment += extra_text

                todo.at[idx, "Commentaar"] = base_comment
                continue
               

            match_res = pd.DataFrame()
            if not results.empty and res_date_col and "staffGID" in results.columns and "CertName" in results.columns:
                mask_r = results["staffGID"] == staff_id
                if cert_name:
                    mask_r &= results["CertName"].astype(str).str.contains(cert_name, case=False, na=False)
                if "Status" in results.columns:
                    mask_r &= results["Status"].astype(str) != "Certified"
                match_res = results[mask_r]

            if not match_res.empty:
                todo.at[idx, "Status"] = "Open"
                todo.at[idx, "Status_Detail"] = "Niet geslaagd"

                base_comment = str(commentaar or "").strip()
                try:
                    fail_date = pd.to_datetime(match_res[res_date_col].iloc[0]).date()
                    extra = f"Niet geslaagd op {fail_date}; herinschrijving nodig."
                except Exception:
                    extra = "Niet geslaagd; herinschrijving nodig."

                new_comment = self.add_unique_comment(base_comment, extra)
                todo.at[idx, "Commentaar"] = new_comment

        self.df["todo"] = todo

    def find_replacement_candidates(
        self,
        current_task: pd.Series,
        months_ahead: int = 6,
    ) -> List[Dict[str, Any]]:
        """
        Zoekt vervangkandidaten voor een taak.
        """
        candidates: List[Dict[str, Any]] = []

        todo = self.df.get("todo", pd.DataFrame())
        req = self.df.get("training_req", pd.DataFrame())
        staff = self.df.get("staff", pd.DataFrame())

        id_col = self.get_id_column() or "staffGID"

        current_id = str(
            current_task.get("staffGID")
            or current_task.get("MedewerkerID")
            or current_task.get(id_col)
            or ""
        ).strip()

        cert_name = str(current_task.get("CertName", "") or "")
        cert_norm = str(current_task.get("CertName_norm", "") or "").strip()
        if not cert_norm:
            cert_norm = self.normalize_certname(cert_name)

        today = pd.Timestamp.today().normalize()
        horizon = today + pd.DateOffset(months=months_ahead)

        staff_map: Dict[str, Dict[str, str]] = {}
        name_col_staff = None

        if staff is not None and not staff.empty and id_col in staff.columns:
            for c in ["FullName", "Name+Firstname", "Employee_Name", "Naam"]:
                if c in staff.columns:
                    name_col_staff = c
                    break

            for _, r in staff.iterrows():
                sid = str(r.get(id_col, "")).strip()
                if not sid:
                    continue

                name = str(r.get(name_col_staff, "") or "") if name_col_staff else ""
                cc = str(
                    r.get("staffCOSTCENTER315", "")
                    or r.get("CostCenter", "")
                    or ""
                )
                pool = str(
                    r.get("Service", "")
                    or r.get("Pool", "")
                    or r.get("OA", "")
                    or ""
                )

                staff_map[sid] = {
                    "name": name,
                    "costcenter": cc,
                    "pool": pool,
                }

        def get_staff_info(
            sid: str,
            fallback_name: str = "",
            fallback_cc: str = "",
            fallback_pool: str = "",
        ) -> tuple:
            info = staff_map.get(str(sid).strip())
            if info:
                name = info.get("name") or fallback_name
                cc = info.get("costcenter") or fallback_cc
                pool = info.get("pool") or fallback_pool
                return name, cc, pool
            return fallback_name, fallback_cc, fallback_pool

        if not req.empty and "CertName" in req.columns:
            df_req = req.copy()

            if "staffGID" in df_req.columns:
                id_col_req = "staffGID"
            elif id_col in df_req.columns:
                id_col_req = id_col
            else:
                id_col_req = None

            if id_col_req:
                df_req[id_col_req] = df_req[id_col_req].astype(str).str.strip()

            df_req["CertName_norm"] = df_req["CertName"].astype(str).apply(
                self.normalize_certname
            )

            mask = df_req["CertName_norm"] == cert_norm
            if current_id and id_col_req:
                mask &= df_req[id_col_req] != current_id

            date_col = None
            for c in ["ScheduledDateParsed", "Planned_Date", "ScheduledDate", "PlannedDate"]:
                if c in df_req.columns:
                    df_req[c] = pd.to_datetime(df_req[c], errors="coerce")
                    date_col = c
                    break

            if date_col:
                d = df_req[date_col]
                mask &= d.notna() & (d >= today) & (d <= horizon)

            df_req = df_req[mask]

            if not df_req.empty:
                name_col_req = detect_name_column(df_req)
                for _, r in df_req.iterrows():
                    sid = str(r.get(id_col_req, "")).strip() if id_col_req else ""
                    d = r.get(date_col)
                    date_str = ""
                    if isinstance(d, pd.Timestamp) and not pd.isna(d):
                        date_str = d.date().isoformat()

                    fallback_name = str(r.get(name_col_req or "", "") or "")
                    fallback_cc = str(r.get("CostCenter", "") or r.get("OA", "") or "")
                    fallback_pool = str(
                        r.get("Service", "")
                        or r.get("Pool", "")
                        or r.get("OA", "")
                        or ""
                    )

                    name, cc, pool = get_staff_info(sid, fallback_name, fallback_cc, fallback_pool)
                    loc = str(r.get("Location", "") or r.get("Locatie", "") or "")

                    candidates.append(
                        {
                            "staff_id": sid,
                            "name": name,
                            "costcenter": cc,
                            "pool": pool,
                            "type": "Binnen 6 maanden ingepland",
                            "date": date_str,
                            "location": loc,
                            "source": "training_req",
                        }
                    )

        if not todo.empty and "CertName" in todo.columns and "Status" in todo.columns:
            df_todo = todo.copy()

            if "staffGID" in df_todo.columns:
                id_col_todo = "staffGID"
            elif id_col in df_todo.columns:
                id_col_todo = id_col
            else:
                id_col_todo = None

            if id_col_todo:
                df_todo[id_col_todo] = df_todo[id_col_todo].astype(str).str.strip()

            df_todo["CertName_norm"] = df_todo["CertName"].astype(str).apply(
                self.normalize_certname
            )

            same_cert = df_todo["CertName_norm"] == cert_norm
            status_series = df_todo["Status"].astype(str).str.lower()

            if "Ingeschreven_Datum" in df_todo.columns:
                df_todo["Ingeschreven_Datum"] = pd.to_datetime(
                    df_todo["Ingeschreven_Datum"], errors="coerce"
                )
            else:
                df_todo["Ingeschreven_Datum"] = pd.NaT

            cur_raw = current_task.get("Ingeschreven_Datum", pd.NaT)
            cur_ts = pd.to_datetime(cur_raw, errors="coerce")
            if isinstance(cur_ts, pd.Timestamp) and not pd.isna(cur_ts):
                cur_date_norm = cur_ts.normalize()
            else:
                cur_date_norm = None

            if id_col_todo:
                mask_ins = same_cert & (status_series == "ingeschreven")
                if current_id:
                    mask_ins &= df_todo[id_col_todo] != current_id
                if cur_date_norm is not None:
                    mask_ins &= (
                        df_todo["Ingeschreven_Datum"].isna()
                        | (df_todo["Ingeschreven_Datum"].dt.normalize() != cur_date_norm)
                    )

                df_ins = df_todo[mask_ins]
                for _, r in df_ins.iterrows():
                    sid = str(r.get(id_col_todo, "")).strip()
                    d = r.get("Ingeschreven_Datum")
                    date_str = ""
                    if isinstance(d, pd.Timestamp) and not pd.isna(d):
                        date_str = d.date().isoformat()

                    fallback_name = str(r.get("MedewerkerNaam", "") or "")
                    fallback_cc = str(r.get("CostCenter", "") or "")
                    fallback_pool = str(
                        r.get("Service", "")
                        or r.get("Pool", "")
                        or r.get("OA", "")
                        or ""
                    )

                    name, cc, pool = get_staff_info(sid, fallback_name, fallback_cc, fallback_pool)
                    loc = str(r.get("Ingeschreven_Locatie", "") or "")

                    candidates.append(
                        {
                            "staff_id": sid,
                            "name": name,
                            "costcenter": cc,
                            "pool": pool,
                            "type": "Reeds ingeschreven (andere dag)",
                            "date": date_str,
                            "location": loc,
                            "source": "todo",
                        }
                    )

            if id_col_todo:
                mask_open = same_cert & (status_series == "open")
                if current_id:
                    mask_open &= df_todo[id_col_todo] != current_id

                df_open = df_todo[mask_open]
                for _, r in df_open.iterrows():
                    sid = str(r.get(id_col_todo, "")).strip()
                    fallback_name = str(r.get("MedewerkerNaam", "") or "")
                    fallback_cc = str(r.get("CostCenter", "") or "")
                    fallback_pool = str(
                        r.get("Service", "")
                        or r.get("Pool", "")
                        or r.get("OA", "")
                        or ""
                    )

                    name, cc, pool = get_staff_info(sid, fallback_name, fallback_cc, fallback_pool)
                    loc = str(
                        r.get("PlannedLocation", "")
                        or r.get("Ingeschreven_Locatie", "")
                        or ""
                    )

                    candidates.append(
                        {
                            "staff_id": sid,
                            "name": name,
                            "costcenter": cc,
                            "pool": pool,
                            "type": "Open in planner (nog niet ingeschreven)",
                            "date": "",
                            "location": loc,
                            "source": "todo",
                        }
                    )

        current_cc = str(current_task.get("CostCenter", "") or "").strip()
        for c in candidates:
            cc = str(c.get("costcenter", "") or "").strip()
            c["same_costcenter"] = bool(current_cc and cc == current_cc)

        def sort_key(c: Dict[str, Any]):
            return (
                0 if c.get("same_costcenter") else 1,
                c.get("type", ""),
                c.get("date", ""),
                c.get("name", ""),
            )

        candidates.sort(key=sort_key)
        return candidates

    def apply_costcenter_filter(self, active_costcenter: str | None):
        """
        Apply kostenplaats filter.
        
        V2-FIX: CostCenter kolomnaam aangepast om te werken na kolom-hernoemen in load_all().
        """
        self.active_costcenter = active_costcenter

        # V2-FIX: Bepaal welke CostCenter kolom we moeten gebruiken
        def get_cc_col(df):
            for c in ("CostCenter", "staffCOSTCENTER315"):
                if c in df.columns:
                    return c
            return None

        if "staff" in self.df:
            staff = self.df["staff"]
            cc_col = get_cc_col(staff)
            
            if active_costcenter and cc_col: 
                staff = staff[
                    staff[cc_col].astype(str).str.strip()
                    == str(active_costcenter).strip()
                ].copy()
                self.df["staff"] = staff

        if "todo" in self.df and "staff" in self. df:
            todo_df = self. df["todo"]
            staff_df = self.df["staff"]

            if not todo_df.empty and not staff_df.empty:
                id_col_cc = self.get_id_column()
                cc_col_staff = get_cc_col(staff_df)
                
                if (
                    id_col_cc
                    and id_col_cc in todo_df.columns
                    and id_col_cc in staff_df.columns
                    and cc_col_staff
                ):
                    todo_df = todo_df.drop(columns=["CostCenter"], errors="ignore")
                    todo_df = todo_df.merge(
                        staff_df[[id_col_cc, cc_col_staff]],
                        on=id_col_cc,
                        how="left",
                    )
                    # V2-FIX: Hernoem naar CostCenter ongeacht de originele naam
                    if cc_col_staff != "CostCenter": 
                        todo_df.rename(
                            columns={cc_col_staff:  "CostCenter"},
                            inplace=True,
                        )

                    self.df["todo"] = todo_df

        if active_costcenter and "todo" in self.df:
            todo = self.df["todo"]
            if "CostCenter" in todo.columns:
                todo = todo[
                    todo["CostCenter"].astype(str).str.strip() 
                    == str(active_costcenter).strip()
                ].copy()
            self.df["todo"] = todo
   
    
    def enrich_todo_with_staff_info(self) -> None:
        """
        UI-compat helper (wordt door main_window.py aangeroepen):
        vult ontbrekende staff-info in de todo-lijst aanvullen op basis van de staff tabel. 

        - Vul (indien leeg): MedewerkerNaam, staffSAPNR, MedewerkerID
        - OVERSCHRIJFT GEEN COSTCENTER MEER (handmatige costcenters blijven behouden bij afdelingswissel)
        
        V2-FIX: CostCenter lookup voorbereid voor beide mogelijke kolomnamen (indien ooit geactiveerd).
        """
        import pandas as pd

        todo = self.df. get("todo", pd.DataFrame())
        staff = self.df.get("staff", pd. DataFrame())
        if todo is None or todo.empty or staff is None or staff.empty:
            return

        # Bepaal join kolom (voorkeur staffGID)
        id_col = self.get_id_column() or "staffGID"
        join_col = None
        for c in ("staffGID", "staffSAPNR", id_col):
            if c in todo. columns and c in staff.columns:
                join_col = c
                break
        if not join_col: 
            return

        # Kandidaten in staff
        name_col = None
        for c in ("FullName", "Name+Firstname", "Employee_Name", "Naam", "MedewerkerNaam"):
            if c in staff.columns:
                name_col = c
                break

        sap_col = None
        for c in ("staffSAPNR", "SAPNR", "PersNr", "PersoneelsNr"):
            if c in staff. columns:
                sap_col = c
                break

        # V2-FIX: CostCenter kolom zoeken in beide mogelijke namen (voor toekomstig gebruik)
        # cc_col = None
        # for c in ("CostCenter", "staffCOSTCENTER315"):  # <-- Volgorde aangepast! 
        #     if c in staff.columns:
        #         cc_col = c
        #         break

        # Prepare subsets (dedupe staff op join)
        staff_sub_cols = [join_col]
        if name_col:
            staff_sub_cols. append(name_col)
        if sap_col and sap_col not in staff_sub_cols: 
            staff_sub_cols.append(sap_col)
        # if cc_col and cc_col not in staff_sub_cols:
        #     staff_sub_cols.append(cc_col)

        staff_sub = staff[staff_sub_cols].copy()
        staff_sub[join_col] = staff_sub[join_col].astype(str).str.strip()
        staff_sub = staff_sub.drop_duplicates(subset=[join_col], keep="first")

        todo2 = todo. copy()
        todo2[join_col] = todo2[join_col].astype(str).str.strip()

        merged = todo2.merge(staff_sub, on=join_col, how="left", suffixes=("", "_staff"))

        def _is_blank(v) -> bool:
            if v is None: 
                return True
            try:
                if pd.isna(v):
                    return True
            except Exception:
                pass
            s = str(v).strip()
            return (s == "") or (s.lower() == "nan")

        # MedewerkerNaam
        if "MedewerkerNaam" not in merged.columns:
            merged["MedewerkerNaam"] = ""
        if name_col: 
            staff_name_series = merged[name_col]
            mask = merged["MedewerkerNaam"].apply(_is_blank)
            merged.loc[mask, "MedewerkerNaam"] = staff_name_series.loc[mask].astype(str).fillna("").map(
                lambda x: "" if str(x).strip().lower() == "nan" else str(x).strip()
            )

        # staffSAPNR
        if "staffSAPNR" not in merged.columns:
            merged["staffSAPNR"] = ""
        if sap_col:
            mask = merged["staffSAPNR"].apply(_is_blank)
            merged.loc[mask, "staffSAPNR"] = merged[sap_col].loc[mask].astype(str).fillna("").map(
                lambda x: "" if str(x).strip().lower() == "nan" else str(x).strip()
            )

        # MedewerkerID:  fallback op staffGID/ID kolom als leeg
        if "MedewerkerID" not in merged.columns:
            merged["MedewerkerID"] = ""
        if "staffGID" in merged.columns:
            mask = merged["MedewerkerID"].apply(_is_blank)
            merged.loc[mask, "MedewerkerID"] = merged["staffGID"].loc[mask].astype(str).fillna("").map(
                lambda x: "" if str(x).strip().lower() == "nan" else str(x).strip()
            )

        # Opruimen extra staff kolommen (behalve join)
        drop_cols = []
        if name_col and name_col not in ("MedewerkerNaam", join_col):
            drop_cols.append(name_col)
        if sap_col and sap_col not in ("staffSAPNR", join_col) and sap_col in merged.columns:
            drop_cols.append(sap_col)
        # if cc_col and cc_col not in ("CostCenter", join_col) and cc_col in merged. columns:
        #     drop_cols. append(cc_col)
        merged.drop(columns=[c for c in drop_cols if c in merged.columns], inplace=True, errors="ignore")

        self.df["todo"] = merged
        print("   ✅ enrich_todo_with_staff_info: todo verrijkt (CostCenter ongewijzigd gelaten)")
    
    def remove_duplicate_tasks(self) -> int:
        """
        UI-compat helper:
        Verwijdert dubbele taken uit de in-memory todo dataframe.

        Dedup-sleutel (best effort):
        - medewerker id (voorkeur: staffGID / MedewerkerID / staffSAPNR)
        - taaktype (TaskType)
        - genormaliseerde naam (voorkeur: CertName_norm / Competence_norm / normalize(CertName|Competence))

        Keuze welke rij te behouden:
        - voorkeur voor rijen met TaskID
        - daarna meest recente LastUpdatedAt, dan CreatedAt

        Returns: aantal verwijderde rijen.
        """
        import pandas as pd

        todo = self.df.get("todo", pd.DataFrame())
        if todo is None or todo.empty:
            return 0

        df = todo.copy()

        # bepaal id kolom
        id_candidates = ["staffGID", "MedewerkerID", "staffSAPNR", self.get_id_column()]
        id_col = None
        for c in id_candidates:
            if c and c in df.columns:
                id_col = c
                break
        if not id_col:
            return 0

        # taaktype kolom
        task_type_col = "TaskType" if "TaskType" in df.columns else None

        # naam kolommen
        norm = getattr(self, "normalize_certname", lambda x: str(x).strip().lower())

        # zorg dat we een norm kolom hebben om op te dedupen
        if "CertName_norm" in df.columns:
            name_norm_col = "CertName_norm"
        elif "Competence_norm" in df.columns:
            name_norm_col = "Competence_norm"
        else:
            # bouw best effort norm vanuit CertName/Competence
            src_col = None
            for c in ("CertName", "Competence"):
                if c in df.columns:
                    src_col = c
                    break
            if not src_col:
                return 0
            name_norm_col = "_Name_norm_tmp"
            df[name_norm_col] = df[src_col].astype(str).apply(norm)

        # normaliseer id strings
        df[id_col] = df[id_col].astype(str).str.strip()

        # sorteer zodat "beste" rij eerst staat
        def _has_taskid(val) -> int:
            try:
                if pd.isna(val):
                    return 0
            except Exception:
                pass
            s = str(val).strip()
            return 0 if (s == "" or s.lower() == "nan") else 1

        if "TaskID" in df.columns:
            df["_has_taskid"] = df["TaskID"].apply(_has_taskid)
        else:
            df["_has_taskid"] = 0

        for dt_col in ("LastUpdatedAt", "CreatedAt"):
            if dt_col in df.columns:
                df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")

        sort_cols = ["_has_taskid"]
        sort_asc = [False]
        if "LastUpdatedAt" in df.columns:
            sort_cols.append("LastUpdatedAt")
            sort_asc.append(False)
        if "CreatedAt" in df.columns:
            sort_cols.append("CreatedAt")
            sort_asc.append(False)

        df = df.sort_values(sort_cols, ascending=sort_asc, kind="mergesort")
        
        # Vervang door (voeg Status toe):
        dedup_cols = [id_col, name_norm_col, "Status"]
        #dedup_cols = [id_col, name_norm_col]
        if task_type_col:
            dedup_cols.append(task_type_col)

        before = len(df)
        df = df.drop_duplicates(subset=dedup_cols, keep="first").copy()
        removed = before - len(df)

        # cleanup helper cols
        df.drop(columns=["_has_taskid"], inplace=True, errors="ignore")
        if name_norm_col == "_Name_norm_tmp":
            df.drop(columns=[name_norm_col], inplace=True, errors="ignore")

        if removed > 0:
            self.df["todo"] = df
            print(f"   🧹 remove_duplicate_tasks: {before} → {len(df)} (verwijderd: {removed})")
        else:
            self.df["todo"] = todo

        return removed

    def remove_duplicate_configs(self) -> int:
        """
        UI-compat helper (wordt aangeroepen in main_window.py):
        Dedupe in-memory dataframes voor config/certificates/training_req zodat
        dubbele rijen niet voor rare sync-issues zorgen.

        Returns: totaal aantal verwijderde rijen.
        """
        import pandas as pd

        removed_total = 0
        norm = getattr(self, "normalize_certname", lambda x: str(x).strip().lower())
        id_col = self.get_id_column() or "staffGID"

        def _to_dt(s):
            return pd.to_datetime(s, errors="coerce")

        # --- Config certificaten
        cfg = self.df.get("config_cert", pd.DataFrame())
        if cfg is not None and not cfg.empty:
            df = cfg.copy()
            if id_col in df.columns:
                df[id_col] = df[id_col].astype(str).str.strip()
            if "CertName_norm" not in df.columns and "CertName" in df.columns:
                df["CertName_norm"] = df["CertName"].astype(str).apply(norm)

            sort_col = None
            for c in ("LaatsteWijziging", "LastUpdatedAt", "CreatedAt"):
                if c in df.columns:
                    df[c] = _to_dt(df[c])
                    sort_col = c
                    break

            before = len(df)
            if sort_col:
                df = df.sort_values(sort_col, ascending=False, kind="mergesort")
            df = df.drop_duplicates(subset=[id_col, "CertName_norm"], keep="first")
            removed_total += before - len(df)

            self.df["config_cert"] = df
            self.df["config"] = df  # alias consistent houden

        # --- Config competenties
        ccfg = self.df.get("competence_config", pd.DataFrame())
        if ccfg is not None and not ccfg.empty:
            df = ccfg.copy()
            if id_col in df.columns:
                df[id_col] = df[id_col].astype(str).str.strip()
            if "Competence_norm" not in df.columns and "Competence" in df.columns:
                df["Competence_norm"] = df["Competence"].astype(str).apply(norm)

            sort_col = None
            for c in ("LaatsteWijziging", "LastUpdatedAt", "CreatedAt"):
                if c in df.columns:
                    df[c] = _to_dt(df[c])
                    sort_col = c
                    break

            before = len(df)
            if sort_col:
                df = df.sort_values(sort_col, ascending=False, kind="mergesort")
            df = df.drop_duplicates(subset=[id_col, "Competence_norm"], keep="first")
            removed_total += before - len(df)

            self.df["competence_config"] = df

        # --- Certificates: behoud nieuwste expiry per medewerker+cert
        certs = self.df.get("certificates", pd.DataFrame())
        if certs is not None and not certs.empty:
            df = certs.copy()
            if id_col in df.columns:
                df[id_col] = df[id_col].astype(str).str.strip()
            if "CertName_norm" not in df.columns and "CertName" in df.columns:
                df["CertName_norm"] = df["CertName"].astype(str).apply(norm)

            exp_col = None
            for c in ("Expiry_Date", "ExpiryDate", "Valid_Until", "Geldig_tot", "Geldig_Tot"):
                if c in df.columns:
                    exp_col = c
                    df[c] = _to_dt(df[c])
                    break

            before = len(df)
            if exp_col:
                df = df.sort_values(exp_col, ascending=False, kind="mergesort")
            df = df.drop_duplicates(subset=[id_col, "CertName_norm"], keep="first")
            removed_total += before - len(df)

            self.df["certificates"] = df

        # --- Training requests: voorkom exacte dubbels (staff+cert+datum)
        req = self.df.get("training_req", pd.DataFrame())
        if req is not None and not req.empty:
            df = req.copy()
            rid = None
            for c in ("staffGID", "staffSAPNR", id_col):
                if c in df.columns:
                    rid = c
                    df[rid] = df[rid].astype(str).str.strip()
                    break

            if rid:
                if "CertName_norm" not in df.columns and "CertName" in df.columns:
                    df["CertName_norm"] = df["CertName"].astype(str).apply(norm)

                date_col = None
                for c in ("ScheduledDateParsed", "ScheduledDate", "Planned_Date", "PlannedDate"):
                    if c in df.columns:
                        df[c] = _to_dt(df[c])
                        date_col = c
                        break

                subset = [rid, "CertName_norm"]
                if date_col:
                    subset.append(date_col)

                before = len(df)
                df = df.drop_duplicates(subset=subset, keep="first")
                removed_total += before - len(df)

                self.df["training_req"] = df

        if removed_total > 0:
            print(f"   🧹 remove_duplicate_configs: verwijderd totaal {removed_total} duplicaten")

        return removed_total

    def close_finished_tasks(self):
        """
        MASTER FUNCTIE V15 (STRIKTE AFDELINGBEVEILIGING):
        1. CHECK A: Ruimt taken op die 'Niet meer nodig' zijn.
        2. CHECK B: Verwerkt resultaten (Geslaagd = Dicht, Gezakt = Open).
        
        🛡️ FIX: Verwerkt uitsluitend medewerkers van de ACTIEVE afdeling.
        """
        import pandas as pd
        from datetime import datetime
        
        print("\n" + "="*60)
        print(f"🔄 close_finished_tasks() V15 - Filter: {self.active_costcenter}")
        
        if "todo" not in self.df or self.df["todo"].empty:
            return
        
        # Data ophalen
        todo = self.df["todo"]
        staff = self.df.get("staff", pd.DataFrame())
        cert_results = self.df.get("cert_results", pd.DataFrame())

        # 1. Bepaal wie er ECHT bij jouw afdeling horen (staff is al gefilterd in load_all)
        id_col = self.get_id_column() or "staffGID"
        my_department_gids = set(staff[id_col].astype(str).str.strip().unique())
        
        if not my_department_gids:
            print("   ⚠️ Geen actieve medewerkers gevonden voor deze afdeling. Skip update.")
            return

        # 2. KOLOMMEN BEPALEN VOOR RESULTATEN
        res_id_col = "staffGID"
        if res_id_col not in cert_results.columns:
            res_id_col = next((c for c in ["staffSAPNR", "MedewerkerID", "PersonID"] if c in cert_results.columns), None)

        res_cert_col = "CertName"
        if res_cert_col not in cert_results.columns:
            res_cert_col = next((c for c in ["Certificaat", "Opleiding", "Training"] if c in cert_results.columns), None)
            
        res_status_col = "Status"
        if res_status_col not in cert_results.columns:
             res_status_col = next((c for c in ["Resultaat", "Result"] if c in cert_results.columns), None)

        res_date_col = "Behaald"
        if res_date_col not in cert_results.columns:
            res_date_col = next((c for c in ["Behaald_Datum", "Achieved_On", "ExamDate"] if c in cert_results.columns), None)
        
        # 3. LOGICA STARTEN
        updates = 0
        now = datetime.now()

        def is_failed(s): 
            return str(s).strip().lower() in ['failed', 'niet geslaagd', 'onvoldoende', 'fail', 'zakte', 'gefaald']
        
        def is_passed(s): 
            return str(s).strip().lower() in ['passed', 'geslaagd', 'certified', 'ok', 'voldoende', 'behaald']

        # Loop door de todo lijst
        for idx, row in todo.iterrows():
            staff_id = str(row.get("staffGID", "")).strip()

            # 🔥 DE VEILIGHEIDSCHECK:
            # Sla medewerkers over die niet tot jouw huidige afdeling behoren.
            if staff_id not in my_department_gids:
                continue

            current_status = str(row.get("Status", "")).strip().lower()
            current_detail = str(row.get("Status_Detail", "")).strip()
            
            if current_status in ["geweigerd", "afwezig (ziekte)", "on hold"]:
                continue
            
            # Bepaal cert_name voor matching
            cert_name = row.get("CertName_norm")
            if not cert_name and row.get("CertName"):
                cert_name = self.normalize_certname(row.get("CertName"))

            # ═══════════════════════════════════════════════════════════
            # CHECK A: CONFIG CLEANUP (Nodig = False?)
            # ═══════════════════════════════════════════════════════════
            is_nodig = row.get("Nodig")
            if (is_nodig is False or is_nodig == 0) and current_status not in ["afgewerkt", "geweigerd"]:
                 inschrijf_dt = pd.to_datetime(row.get("Ingeschreven_Datum", pd.NaT), errors="coerce")
                 created_by = str(row.get("CreatedBy", "") or "")

                 # Bescherm ingeschreven taken of herkansingen
                 if current_status == "ingeschreven" or pd.notna(inschrijf_dt):
                     pass 
                 elif "niet geslaagd" in current_detail.lower() or "herinschrijving" in current_detail.lower():
                     pass 
                 elif created_by == "sync_failed_results_to_todo":
                     pass
                 else:
                     todo.at[idx, "Status"] = "Afgewerkt"
                     todo.at[idx, "Status_Detail"] = "Niet meer vereist in config"
                     todo.at[idx, "LastUpdatedAt"] = now
                     updates += 1
                     continue 

            # ═══════════════════════════════════════════════════════════
            # CHECK B: RESULTATEN VERWERKING (Passed/Failed?)
            # ═══════════════════════════════════════════════════════════
            if cert_results.empty or not res_id_col or not cert_name:
                continue

            matches = cert_results[
                (cert_results[res_id_col].astype(str).str.strip() == staff_id) & 
                (cert_results[res_cert_col].apply(self.normalize_certname) == cert_name)
            ]

            if not matches.empty:
                if res_date_col and res_date_col in matches.columns:
                    matches = matches.sort_values(res_date_col, ascending=False)
                
                last_res = matches.iloc[0]
                res_stat = str(last_res.get(res_status_col, ""))
                behaald_dt = pd.to_datetime(last_res.get(res_date_col, pd.NaT))
                datum_str = behaald_dt.strftime('%d-%m-%Y') if pd.notna(behaald_dt) else "?"

                # SCENARIO: GESLAAGD
                if is_passed(res_stat):
                    if current_status != 'afgewerkt':
                        todo.at[idx, "Status"] = "Afgewerkt"
                        todo.at[idx, "Status_Detail"] = f"Behaald op {datum_str}"
                        todo.at[idx, "Behaald_Datum"] = behaald_dt
                        todo.at[idx, "LastUpdatedAt"] = now
                        updates += 1

                # SCENARIO: NIET GESLAAGD
                elif is_failed(res_stat):
                    if current_status != 'open' or "niet geslaagd" not in current_detail.lower():
                        todo.at[idx, "Status"] = "Open"
                        todo.at[idx, "Status_Detail"] = f"Niet geslaagd ({datum_str}) - herinschrijving nodig"
                        todo.at[idx, "Ingeschreven_Datum"] = pd.NaT 
                        todo.at[idx, "LastUpdatedAt"] = now
                        updates += 1

        if updates > 0:
            self.df["todo"] = todo
            if self.USE_SQL_FOR_TODO:
                self.save_todo_planner()
                
        print(f"✅ close_finished_tasks: {updates} taken bijgewerkt voor {self.active_costcenter}.")
    
    def detect_absent_from_completed_training(self):
        """
        Detecteert mensen die afwezig waren bij een opleiding die al is afgelopen.
        
        Logica:
        - Groepeert "Ingeschreven" taken op CertName + Ingeschreven_Datum
        - Als er resultaten zijn voor sommigen maar niet voor anderen
        - En de opleidingsdatum is verstreken (> 1 dag geleden)
        - Markeer de mensen zonder resultaat als "afwezig (ziekte)"
        
        Dit lost het probleem op waarbij iemand ziek is en geen resultaat heeft,
        maar de opleiding wel is afgelopen (omdat anderen wel resultaten hebben).
        """
        import pandas as pd
        from datetime import datetime
        
        print("\n" + "="*60)
        print(f"🔍 detect_absent_from_completed_training() - Filter: {self.active_costcenter}")
        
        if "todo" not in self.df or self.df["todo"].empty:
            return
        
        todo = self.df["todo"]
        cert_results = self.df.get("cert_results", pd.DataFrame())
        staff = self.df.get("staff", pd.DataFrame())
        
        if cert_results.empty:
            print("   ℹ️ Geen cert_results beschikbaar - skip detectie")
            return
        
        # Bepaal wie er bij jouw afdeling horen
        id_col = self.get_id_column() or "staffGID"
        my_department_gids = set(staff[id_col].astype(str).str.strip().unique()) if not staff.empty else set()
        
        # Kolommen bepalen voor resultaten
        res_id_col = "staffGID"
        if res_id_col not in cert_results.columns:
            res_id_col = next((c for c in ["staffSAPNR", "MedewerkerID", "PersonID"] if c in cert_results.columns), None)
        
        res_cert_col = "CertName"
        if res_cert_col not in cert_results.columns:
            res_cert_col = next((c for c in ["Certificaat", "Opleiding", "Training"] if c in cert_results.columns), None)
        
        if not res_id_col or not res_cert_col:
            print("   ⚠️ Kan resultaten kolommen niet bepalen - skip detectie")
            return
        
        # Filter op "Ingeschreven" taken met verstreken datum
        today = pd.Timestamp.today().normalize()
        ingeschreven_tasks = todo[
            (todo["Status"].astype(str).str.strip().str.lower() == "ingeschreven") &
            (todo["Ingeschreven_Datum"].notna())
        ].copy()
        
        if ingeschreven_tasks.empty:
            print("   ℹ️ Geen ingeschreven taken met datum gevonden")
            return
        
        # Converteer datum naar datetime
        ingeschreven_tasks["Ingeschreven_Datum"] = pd.to_datetime(ingeschreven_tasks["Ingeschreven_Datum"], errors="coerce")
        
        # Filter op verstreken datums (> 1 dag geleden)
        ingeschreven_tasks["days_ago"] = (today - ingeschreven_tasks["Ingeschreven_Datum"]).dt.days
        verstreken_tasks = ingeschreven_tasks[ingeschreven_tasks["days_ago"] > 1].copy()
        
        if verstreken_tasks.empty:
            print("   ℹ️ Geen verstreken ingeschreven taken gevonden")
            return
        
        # Normaliseer certnamen
        verstreken_tasks["CertName_norm"] = verstreken_tasks["CertName"].astype(str).apply(self.normalize_certname)
        
        # Groepeer op CertName_norm + Ingeschreven_Datum (genormaliseerd naar datum zonder tijd)
        verstreken_tasks["Training_Key"] = (
            verstreken_tasks["CertName_norm"].astype(str) + "|" + 
            verstreken_tasks["Ingeschreven_Datum"].dt.date.astype(str)
        )
        
        updates = 0
        now = datetime.now()
        
        # Loop door elke unieke training (CertName + Datum combinatie)
        for training_key, group in verstreken_tasks.groupby("Training_Key"):
            if len(group) < 2:
                continue  # Skip als er maar 1 persoon is ingeschreven
            
            # Check of er resultaten zijn voor deze training
            cert_norm = group["CertName_norm"].iloc[0]
            training_date = group["Ingeschreven_Datum"].iloc[0]
            
            # Zoek resultaten voor deze training (binnen 7 dagen van de opleidingsdatum)
            training_results = cert_results[
                (cert_results[res_cert_col].apply(self.normalize_certname) == cert_norm) &
                (pd.to_datetime(cert_results.get("Behaald", pd.NaT), errors="coerce").notna())
            ].copy()
            
            if training_results.empty:
                continue  # Geen resultaten voor deze training
            
            # Check welke mensen uit de groep resultaten hebben
            people_with_results = set()
            for _, result_row in training_results.iterrows():
                result_staff_id = str(result_row.get(res_id_col, "")).strip()
                result_date = pd.to_datetime(result_row.get("Behaald", pd.NaT), errors="coerce")
                
                # Check of resultaat binnen 7 dagen van opleidingsdatum valt
                if pd.notna(result_date) and pd.notna(training_date):
                    days_diff = abs((result_date.normalize() - training_date.normalize()).days)
                    if days_diff <= 7:
                        people_with_results.add(result_staff_id)
            
            # Als er resultaten zijn voor sommigen maar niet voor allen
            if len(people_with_results) > 0:
                # Markeer mensen zonder resultaat als afwezig
                for idx, task_row in group.iterrows():
                    staff_id = str(task_row.get("staffGID", "")).strip()
                    
                    # Skip als niet van deze afdeling
                    if my_department_gids and staff_id not in my_department_gids:
                        continue
                    
                    # Skip als al afgewerkt of andere status
                    current_status = str(task_row.get("Status", "")).strip().lower()
                    if current_status not in ["ingeschreven"]:
                        continue
                    
                    # Skip als deze persoon wel een resultaat heeft
                    if staff_id in people_with_results:
                        continue
                    
                    # Markeer als afwezig (ziekte)
                    todo.at[idx, "Status"] = "Afwezig (ziekte)"
                    todo.at[idx, "Status_Detail"] = f"Opleiding afgelopen op {training_date.strftime('%d-%m-%Y')} - geen resultaat (waarschijnlijk afwezig)"
                    todo.at[idx, "LastUpdatedAt"] = now
                    updates += 1
                    
                    print(f"   🏥 {task_row.get('MedewerkerNaam', staff_id)} gemarkeerd als afwezig voor {cert_norm} op {training_date.strftime('%d-%m-%Y')}")
        
        if updates > 0:
            self.df["todo"] = todo
            if self.USE_SQL_FOR_TODO:
                self.save_todo_planner()
            
        print(f"✅ detect_absent_from_completed_training: {updates} taken gemarkeerd als afwezig voor {self.active_costcenter}.")
    
    def convert_names_to_lastname_first(self):
        """Converteer namen naar correct formaat."""
        todo = self.df.get("todo", pd.DataFrame())
        staff = self.df.get("staff", pd.DataFrame())
        
        if todo.empty or staff.empty:
            return
        
        if "MedewerkerNaam" not in todo.columns:
            return
        
        id_col = self.get_id_column() or "staffGID"
        
        if id_col not in todo.columns or id_col not in staff.columns:
            return
        
        name_lookup = {}
        if "FullName" in staff.columns:
            for _, row in staff.iterrows():
                sid = str(row.get(id_col, "")).strip()
                name = str(row.get("FullName", "")).strip()
                if sid and name and name.lower() != "nan":
                    name_lookup[sid] = name
        
        changed = 0
        for idx, row in todo.iterrows():
            staff_id = str(row.get(id_col, "")).strip()
            current_name = str(row.get("MedewerkerNaam", "")).strip()
            
            if staff_id in name_lookup:
                correct_name = name_lookup[staff_id]
                
                if current_name != correct_name:
                    todo.at[idx, "MedewerkerNaam"] = correct_name
                    changed += 1
        
        if changed > 0:
            self.df["todo"] = todo
            self.save_todo()
            print(f"✅ {changed} medewerkernamen geconverteerd naar 'Achternaam, Voornaam' formaat")
        else:
            print("✅ Alle namen zijn al in correct formaat")

    def get_recent_certified_from_results(self, weeks:  int = 6):
        """
        Geeft een DataFrame terug met recent behaalde certificaten uit cert_results
        (Status = Certified/Geslaagd/Behaald/Passed en Behaald-datum binnen laatste `weeks` weken).

        - Linkt cert_results via staffGID/staffSAPNR aan self.df["staff"] om CostCenter op te halen. 
        - Beperkt tot het actief geselecteerde costcenter (self.active_costcenter), indien aanwezig.
        - Wijzigt GEEN data in self.df (read-only).
        
        V2-FIX: CostCenter lookup volgorde aangepast om te werken na kolom-hernoemen in load_all().
        """

        import pandas as pd
        from datetime import datetime

        cert_results = self. df. get("cert_results", pd.DataFrame())
        staff = self.df. get("staff", pd.DataFrame())

        if cert_results.empty:
            print("DEBUG recent_certified:  cert_results is leeg")
            return pd.DataFrame()

        id_col = self. get_id_column()
        now = datetime.now()
        today = pd.Timestamp(now.date())
        cutoff_date = today - pd. Timedelta(weeks=weeks)

        def is_certified_status(s:  str) -> bool:
            s = (s or "").strip().lower()
            return s in ["certified", "passed", "geslaagd", "behaald", "ok"]

        print("\nDEBUG recent_certified:  rows in cert_results:", len(cert_results))
        print("DEBUG recent_certified: columns:", list(cert_results. columns))
        active_cc = getattr(self, "active_costcenter", None)
        print("DEBUG recent_certified: active_costcenter:", active_cc)

        # ──────────────────────────────────────────────────────────────
        # 1. Kolommen bepalen in cert_results
        # ──────────────────────────────────────────────────────────────
        res_id_col = None
        for c in [id_col, "staffGID", "staffSAPNR"]:
            if c in cert_results. columns:
                res_id_col = c
                break

        res_cert_col = None
        for c in ["CertName", "Certificaat", "Training", "Course"]:
            if c in cert_results. columns:
                res_cert_col = c
                break

        behaald_col = None
        for c in ["Behaald", "Exam_Date", "CompletedDate", "ExamDate"]:
            if c in cert_results. columns:
                behaald_col = c
                break

        geldig_tot_col = None
        for c in ["Geldig_tot", "Geldig_Tot", "Valid_Until", "ExpiryDate", "Expiry_Date", "Einde_Geldigheid"]:
            if c in cert_results.columns:
                geldig_tot_col = c
                break

        naam_col = None
        for c in ["Naam", "Employee_Name", "Name"]:
            if c in cert_results. columns:
                naam_col = c
                break

        service_col = "Service" if "Service" in cert_results.columns else None

        print("DEBUG recent_certified: res_id_col:", res_id_col)
        print("DEBUG recent_certified: res_cert_col:", res_cert_col)
        print("DEBUG recent_certified: behaald_col:", behaald_col)
        print("DEBUG recent_certified: geldig_tot_col:", geldig_tot_col)
        print("DEBUG recent_certified: service_col:", service_col)

        if not (res_id_col and res_cert_col and behaald_col):
            print("DEBUG recent_certified: onvoldoende kolommen om iets terug te geven")
            return pd. DataFrame()

        # ──────────────────────────────────────────────────────────────
        # 2. CostCenter aan cert_results koppelen via staff
        #    V2-FIX:  Zoek eerst naar 'CostCenter' (na hernoemen), dan fallback
        # ──────────────────────────────────────────────────────────────
        costcenter_col_staff = None
        if not staff.empty:
            # V2-FIX: Volgorde aangepast - CostCenter eerst (na hernoemen in load_all)
            for c in ["CostCenter", "staffCOSTCENTER315", "Kostenplaats", "CC"]:
                if c in staff.columns:
                    costcenter_col_staff = c
                    break

        if costcenter_col_staff:
            print(f"DEBUG recent_certified: staff merge via {res_id_col} -> staff['{costcenter_col_staff}']")
            staff_tmp = staff[[res_id_col, costcenter_col_staff]].copy()
            staff_tmp[res_id_col] = staff_tmp[res_id_col].astype(str).str.strip()

            cr_tmp = cert_results. copy()
            cr_tmp[res_id_col] = cr_tmp[res_id_col].astype(str).str.strip()

            merged = pd.merge(
                cr_tmp,
                staff_tmp,
                on=res_id_col,
                how="left",
                suffixes=("", "_staff")
            )
            # V2-FIX: Alleen hernoemen als de kolom niet al CostCenter heet
            if costcenter_col_staff != "CostCenter": 
                merged = merged.rename(columns={costcenter_col_staff: "CostCenter"})
        else:
            print("DEBUG recent_certified:  GEEN CostCenter-kolom in staff, costcenterfilter NIET mogelijk")
            merged = cert_results.copy()
            merged["CostCenter"] = ""

        # ──────────────────────────────────────────────────────────────
        # 3. Filter op actief costcenter (indien beschikbaar)
        # ────────────────────────────────────────────────���─────────────
        if active_cc: 
            cc_vals = merged["CostCenter"].astype(str).str.strip()
            unique_cc = cc_vals.unique()
            print("DEBUG recent_certified:  unieke CostCenter-waarden (eerste 10):", unique_cc[:10])

            active_cc_str = str(active_cc).strip()
            if active_cc_str in unique_cc: 
                merged = merged[cc_vals == active_cc_str]
                print("DEBUG recent_certified: gefilterd op CostCenter =", active_cc_str,
                      "→ rows over:", len(merged))
            else:
                print(f"DEBUG recent_certified: active_costcenter '{active_cc_str}' niet gevonden in CostCenter → GEEN costcenterfilter toegepast")
        else:
            print("DEBUG recent_certified: geen active_costcenter, geen costcenterfilter")

        if merged.empty:
            print("DEBUG recent_certified: na costcenterfilter geen rijen meer")
            return pd.DataFrame()

        # ──────────────────────────────────────────────────────────────
        # 4. Records selecteren: Certified + Behaald binnen laatste `weeks` weken
        # ──────────────────────────────────────────────────────────────
        rows = []
        for _, r in merged.iterrows():
            status_raw = str(r.get("Status", "")).strip()
            if not is_certified_status(status_raw):
                continue

            behaald_dt = pd.to_datetime(r.get(behaald_col), errors="coerce")
            if behaald_dt is None or pd.isna(behaald_dt):
                continue

            if not (cutoff_date <= behaald_dt <= today):
                continue

            staff_id_val = str(r.get(res_id_col, "")).strip()
            cert_name_val = str(r.get(res_cert_col, "")).strip()
            if not staff_id_val or not cert_name_val:
                continue

            cert_norm = self.normalize_certname(cert_name_val)
            geldig_dt = pd.to_datetime(r.get(geldig_tot_col), errors="coerce") if geldig_tot_col else None
            medewerker_naam = str(r.get(naam_col, "")).strip() if naam_col else ""
            service_val = str(r. get(service_col, "")).strip() if service_col else ""
            costcenter_val = str(r.get("CostCenter", "")).strip()

            rows.append({
                "staff_id": staff_id_val,
                "Medewerker": medewerker_naam,
                "Service": service_val,
                "CostCenter": costcenter_val,
                "CertName": cert_name_val,
                "CertName_norm": cert_norm,
                "Status": status_raw,
                "Behaald_Datum": behaald_dt,
                "Geldig_tot": geldig_dt,
            })

        if not rows:
            print("DEBUG recent_certified: geen recente Certified-records gevonden (na costcenterfilter)")
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df = df.sort_values(by="Behaald_Datum", ascending=False).reset_index(drop=True)
        print("DEBUG recent_certified: returning", len(df), "rows")
        return df
    
# ===============================================================
# AANPASSING IN xaurum/core/datastore.py (close_tasks_no_longer_needed)
# ===============================================================

    def close_tasks_no_longer_needed(self):
        """
        Sluit alle Open of Ingeschreven taken in 'todo' af 
        waarvoor de configuratie 'Nodig' op False staat.
        """
        todo = self.df.get("todo", pd.DataFrame())
        cfg_cert = self.df.get("config_cert", pd.DataFrame())
        cfg_comp = self.df.get("competence_config", pd.DataFrame())
        
        if todo.empty or (cfg_cert.empty and cfg_comp.empty):
            return 0
        
        id_col = self.get_id_column() or "staffGID"
        
        # 1. Bouw lookup van alle items die NIET NODIG zijn
        niet_nodig_cert = cfg_cert[cfg_cert["Nodig"].apply(lambda x: not is_truthy_value(x))].copy()
        if "CertName" in niet_nodig_cert.columns:
            niet_nodig_cert["CertName_norm"] = niet_nodig_cert["CertName"].astype(str).apply(self.normalize_certname)
            niet_nodig_cert["TaskType"] = "Certificaat"
        
        niet_nodig_comp = cfg_comp[cfg_comp["Nodig"].apply(lambda x: not is_truthy_value(x))].copy()
        if "Competence" in niet_nodig_comp.columns:
            niet_nodig_comp["CertName_norm"] = niet_nodig_comp["Competence"].astype(str).apply(self.normalize_certname)
            niet_nodig_comp["TaskType"] = "Vaardigheid"
        
        needed_cols = [id_col, "CertName_norm", "TaskType"]
        
        niet_nodig_cert = niet_nodig_cert[needed_cols].copy()
        niet_nodig_comp = niet_nodig_comp[needed_cols].copy()
        
        niet_nodig_keys = pd.concat([niet_nodig_cert, niet_nodig_comp], ignore_index=True)
        niet_nodig_keys = set(tuple(x) for x in niet_nodig_keys.values)
        
        if not niet_nodig_keys:
            return 0

        # 2. Update de taken in de todo lijst (in geheugen)
        modified_count = 0
        now = pd.Timestamp.now()
        
        for idx, row in todo.iterrows():
            current_status = str(row.get("Status", "")).strip().lower()
            task_type = str(row.get("TaskType", "Certificaat")).strip()
            
            if current_status in ["open", "ingeschreven", "on hold", "in wachtrij"]:
                
                staff_id = str(row.get(id_col, "")).strip()
                cert_norm = str(row.get("CertName_norm", "")).strip()
                
                key = (staff_id, cert_norm, task_type)
                
                if key in niet_nodig_keys:
                    # Sluit de taak!
                    todo.at[idx, 'Status'] = 'Afgewerkt'
                    todo.at[idx, 'Status_Detail'] = 'Niet meer Nodig (Config aangepast)'
                    todo.at[idx, 'LastUpdatedAt'] = now
                    modified_count += 1
                    
        if modified_count > 0:
            self.df["todo"] = todo
            # self.save_todo() # <<< DEZE LIJN IS NU VERWIJDERD
            print(f"✅ {modified_count} taken in geheugen afgesloten omdat 'Nodig=False' in config.")
        
        return modified_count
    
    def _normalize_sapnr(self, val) -> str:
        """Standaard SAPNR: digits-only string ZONDER leading zeros. Leeg/ongeldig => ''."""
        return normalize_sapnr(val)


    def _clean_str(self, val) -> str:
        """Uniforme string cleanup voor DataFrames."""
        if val is None:
            return ""
        s = str(val).strip()
        return "" if s.lower() in ("nan", "none", "null") else s    

    def repair_readable_names(self):
        """
        🚑 NOODREPARATIE FUNCTIE:
        Herstelt leesbare namen (bv. 'EA-E-294...') in de Todo-lijst 
        waar deze per ongeluk zijn overschreven door genormaliseerde namen (bv. 'eae294...').
        """
        print("\n🚑 START NAAM REPARATIE...")
        
        todo = self.df.get("todo", pd.DataFrame())
        if todo.empty: return

        # 1. Bouw een woordenboek: { 'eae294...': 'EA-E-294 - BA5...' }
        # We halen de mooie namen uit Config, Master Certs en Catalogus
        lookup = {}
        
        # Bron A: Config (Meest betrouwbaar voor jouw team)
        cfg = self.df.get("config_cert", pd.DataFrame())
        if not cfg.empty and "CertName" in cfg.columns:
            for _, row in cfg.iterrows():
                orig = str(row["CertName"]).strip()
                norm = self.normalize_certname(orig)
                if orig and norm: lookup[norm] = orig

        # Bron B: Master Certificaten
        master = self.df.get("master_cert", pd.DataFrame())
        if not master.empty and "CertName" in master.columns:
            for _, row in master.iterrows():
                orig = str(row["CertName"]).strip()
                norm = self.normalize_certname(orig)
                if orig and norm and norm not in lookup: lookup[norm] = orig

        # Bron C: Training Catalogus
        cat = getattr(self, "training_catalog", pd.DataFrame())
        if not cat.empty:
            col = next((c for c in ["title", "Title", "raw_text"] if c in cat.columns), None)
            if col:
                for _, row in cat.iterrows():
                    orig = str(row[col]).strip()
                    norm = self.normalize_certname(orig)
                    if orig and norm and norm not in lookup: lookup[norm] = orig

        # 2. Pas toe op de Todo lijst
        repaired_count = 0
        for idx, row in todo.iterrows():
            current_name = str(row.get("CertName", "")).strip()
            
            # Check of de naam "kapot" is (geen spaties, alles kleine letters, lijkt op technische key)
            is_broken = (
                len(current_name) > 0 and 
                " " not in current_name and 
                current_name == current_name.lower() and 
                current_name in lookup
            )
            
            if is_broken:
                beautiful_name = lookup[current_name]
                todo.at[idx, "CertName"] = beautiful_name
                repaired_count += 1
        
        if repaired_count > 0:
            self.df["todo"] = todo
            print(f"   ✅ {repaired_count} certificaatnamen hersteld naar leesbaar formaat!")
            # Meteen opslaan om de database te fixen
            if self.USE_SQL_FOR_TODO:
                self.save_todo_planner()
        else:
            print("   ℹ️ Geen namen hoeven gerepareerd te worden.")
    

    def save_todo_planner(self, df_to_save=None):
        """
        V39-POLITIE: Filtert STRIKT op het actieve costcenter. 
        Gooit alles weg wat niet bij de huidige afdeling hoort VOORDAT het naar SQL gaat.
        
        V39-FIX: applymap -> map (pandas 2.1+) en _SrcRowId KeyError fix. 
        """
        import numpy as np
        print("\n" + "="*60)
        print(f"👮 save_todo_planner() - CONTROLE VOOR AFDELING: {self.active_costcenter}")
        
        todo_data = df_to_save if df_to_save is not None else self. df.get("todo", pd.DataFrame())
        
        if todo_data is None or todo_data. empty:
            print("   ⚠️ Geen data om te verwerken.")
            return False

        # 1. 🛡️ GRENSCONTROLE:  Alleen data van actieve costcenter mag door!
        if self.active_costcenter:
            # Maak een strikte kopie
            todo_data["CostCenter"] = todo_data["CostCenter"].astype(str).str.strip()
            target_cc = str(self.active_costcenter).strip()
            
            # FILTER: Behoud alleen rijen waar CostCenter == Active CostCenter
            final_df = todo_data[todo_data["CostCenter"] == target_cc].copy()
            
            verboden = len(todo_data) - len(final_df)
            if verboden > 0:
                print(f"   🚫 POLITIE: {verboden} taken van ANDERE afdelingen tegengehouden!")
                print(f"   ✅ POLITIE: {len(final_df)} taken van {target_cc} goedgekeurd voor opslag.")
        else:
            print("   ⚠️ WAARSCHUWING: Geen actief costcenter ingesteld! Politie inactief.")
            final_df = todo_data. copy()

        if final_df.empty:
            print("   ⚠️ Geen taken over na controle.  Opslaan geannuleerd (geen wijzigingen voor deze afdeling).")
            return True

        # 2. SCHEMA CLEANUP (Competence mag niet naar SQL)
        cols_to_drop = ["Competence", "CertName_display", "_SrcRowId", "_rn"]
        for col in cols_to_drop: 
            if col in final_df.columns:
                final_df = final_df.drop(columns=[col])

        # 3. TYPE CLEANUP (Voorkom float64/lijst errors)
        numeric_cols = ["Geldigheid_maanden", "DaysUntilExpiry", "TaskID"]
        for col in numeric_cols: 
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
        
        # V39-FIX:  Lege lijsten [] -> NaN (applymap is deprecated, gebruik map)
        final_df = final_df.map(lambda x: np.nan if isinstance(x, list) and len(x) == 0 else x)

        # 4. STUUR NAAR MANAGER
        if self.sql_training_manager: 
            try: 
                # Hier roepen we de manager aan (die we in Stap 1 hebben gefixt)
                success, mapping = self.sql_training_manager.save_todo_planner(final_df)
                
                if success:
                    print(f"   💾 SQL:  Opslag geslaagd.")
                    
                    # V39-FIX: Update de lokale TaskIDs met veiligheidscheck op _SrcRowId
                    if mapping and "_SrcRowId" in self.df["todo"].columns:
                        self.df["todo"]["_SrcRowId"] = self.df["todo"]["_SrcRowId"].astype(str)
                        for src_id, new_task_id in mapping. items():
                            mask = self.df["todo"]["_SrcRowId"] == src_id
                            if mask.any():
                                self.df["todo"]. loc[mask, "TaskID"] = new_task_id
                    return True
                else:
                    print("   ❌ SQL Manager gaf False terug.")
            except Exception as e:
                print(f"   ❌ Fout bij aanroepen manager: {e}")
        
        return False
        
    def save_todo(self):
        """
        Wrapper: Dit is de functie die door de UI knop wordt aangeroepen.
        """
        print("\n" + "="*40)
        print("💾 save_todo() Aangeroepen vanuit UI")
        self.save_todo_planner()

    def clean_sql_config_names(self):
        """
        WASTRAAT V2: Vertaalt namen (Frans/LS/HS) in SQL maar voorkomt UNIQUE KEY fouten.
        Maakt gebruik van de MERGE-logica in add_medewerker_config.
        """
        if not self.engine:
            print("   ❌ Geen SQL connectie voor cleanup.")
            return

        print("   🧼 START Config Cleanup (SQL Wasstraat V2)...")
        
        # 1. Laad de vertaalmap uit het geheugen
        mapping = self.df.get("mapping_cert", pd.DataFrame())
        if mapping.empty:
            print("   ⚠️ Geen mapping geladen, cleanup overgeslagen.")
            return

        translation_map = {}
        src_col = next((c for c in ["OrigineleNaam", "OriginalName", "Frans"] if c in mapping.columns), mapping.columns[0])
        dst_col = next((c for c in ["VertaaldeNaam", "DutchName", "Nederlands"] if c in mapping.columns), mapping.columns[1])
        
        for _, row in mapping.iterrows():
            orig = str(row[src_col]).strip()
            target = str(row[dst_col]).strip()
            if orig and target:
                translation_map[self.normalize_certname(orig)] = target

        # 2. Haal alle huidige configuraties op
        try:
            # We hebben ConfigID, staffGID en CertName nodig
            query = "SELECT ConfigID, staffGID, CertName, Nodig FROM dbo.TM_MedewerkerCertificaatConfig"
            df_sql = pd.read_sql(query, self.engine)
            
            repaired = 0
            for _, row in df_sql.iterrows():
                cid = row["ConfigID"]
                gid = row["staffGID"]
                cname = str(row["CertName"])
                nodig_status = bool(row["Nodig"])
                
                # Check of de huidige naam "vervuild" is
                norm = self.normalize_certname(cname)
                
                if norm in translation_map:
                    target_name = translation_map[norm]
                    
                    # Alleen actie als de naam echt anders is
                    if cname != target_name:
                        # 3. GEBRUIK DE VEILIGE MERGE
                        # Dit zorgt ervoor dat als 'target_name' al bestaat, deze wordt geüpdatet.
                        # Als hij niet bestaat, wordt hij aangemaakt.
                        if self.add_medewerker_config(gid, target_name, nodig=nodig_status):
                            # 4. VERWIJDER HET OUDE RECORD
                            # Nu de data veilig 'ge-merged' is in het correcte record, 
                            # kunnen we de foute naam veilig verwijderen.
                            with self.engine.begin() as conn:
                                conn.execute(text("DELETE FROM dbo.TM_MedewerkerCertificaatConfig WHERE ConfigID = :id"), {"id": cid})
                            repaired += 1
            
            if repaired > 0:
                print(f"   ✅ {repaired} records succesvol hersteld en samengevoegd in SQL.")
            else:
                print("   ✅ SQL Config tabel is reeds volledig gestandaardiseerd.")

        except Exception as e:
            print(f"   ❌ Fout tijdens SQL Wasstraat: {e}")
    # ============================================================
    # ACTIES VOOR DISCREPANTIES TRACKER
    # ============================================================

    def fix_add_mapping(self, wrong_name, correct_name):
        """Voegt een nieuwe vertaling toe aan Naammapping en slaat op."""
        print(f"   🛠️ FIX: Mapping toevoegen: '{wrong_name}' -> '{correct_name}'")
        
        # 1. Toevoegen aan DataFrame
        mapping = self.df.get("mapping_cert", pd.DataFrame())
        new_row = {"OrigineleNaam": wrong_name, "VertaaldeNaam": correct_name}
        
        if mapping.empty:
            mapping = pd.DataFrame([new_row])
        else:
            mapping = pd.concat([mapping, pd.DataFrame([new_row])], ignore_index=True)
        
        self.df["mapping_cert"] = mapping
        
        # 2. Opslaan naar Excel (zodat het bewaard blijft!)
        try:
            path = os.path.join(self.base_dir, "config", "Naammapping.xlsx")
            mapping.to_excel(path, index=False)
            print("   💾 Naammapping.xlsx bijgewerkt.")
            
            # 3. Direct toepassen op SQL (Auto-Clean)
            self.clean_sql_config_names()
            return True
        except Exception as e:
            print(f"   ❌ Kon mapping niet opslaan: {e}")
            return False

    def fix_create_config(self, staff_gid, staff_name, cert_name, correct_norm):
        """Maakt een ontbrekende config regel aan in SQL."""
        print(f"   🛠️ FIX: Config aanmaken voor {staff_gid} - {cert_name}")
        if not self.engine: return False
        
        try:
            import pyodbc
            conn = self.engine.raw_connection()
            cursor = conn.cursor()
            
            # Insert query
            sql = """
            INSERT INTO dbo.TM_MedewerkerCertificaatConfig 
            (staffGID, MedewerkerNaam, CertName, CertName_norm, Nodig, DatumToegevoegd, GewijzigdDoor)
            VALUES (?, ?, ?, ?, 1, GETDATE(), 'DiscrepancyFixer')
            """
            cursor.execute(sql, (staff_gid, staff_name, cert_name, correct_norm))
            conn.commit()
            cursor.close(); conn.close()
            
            # Update memory
            self.load_config_from_sql() 
            return True
        except Exception as e:
            print(f"   ❌ SQL Insert fout: {e}")
            return False

    def fix_enable_config(self, staff_gid, cert_norm):
        """Zet een config regel op 'Nodig = 1'."""
        print(f"   🛠️ FIX: Config activeren voor {staff_gid} - {cert_norm}")
        if not self.engine: return False
        
        try:
            import pyodbc
            conn = self.engine.raw_connection()
            cursor = conn.cursor()
            
            sql = """
            UPDATE dbo.TM_MedewerkerCertificaatConfig 
            SET Nodig = 1, LaatsteWijziging = GETDATE(), GewijzigdDoor = 'DiscrepancyFixer'
            WHERE staffGID = ? AND CertName_norm = ?
            """
            cursor.execute(sql, (staff_gid, cert_norm))
            conn.commit()
            cursor.close(); conn.close()
            
            self.load_config_from_sql()
            return True
        except Exception as e:
            print(f"   ❌ SQL Update fout: {e}")
            return False

    # def add_cert_mapping(self, original: str, target: str, source: str = "Manual"):
        # """
        # Voegt een mapping toe aan dbo.TM_NaamMapping en update direct het geheugen.
        # """
        # from sqlalchemy import text
        # import pandas as pd

        # print(f"🔗 add_cert_mapping: '{original}' -> '{target}'")
        
        # # 1. Update direct het Vertaalwoordenboek (Snelheid voor normalisatie)
        # if hasattr(self, "translation_dict"):
            # self.translation_dict[original.strip()] = target.strip()

        # # 2. Update de DataFrame (Zodat je het direct in de UI lijst ziet)
        # df = self.df.get("mapping_cert", pd.DataFrame())
        # new_row = {"OrigineleNaam": original, "VertaaldeNaam": target, "Bron": source}

        # if df.empty:
            # self.df["mapping_cert"] = pd.DataFrame([new_row])
        # else:
            # # Kolommen zoeken (flexibel)
            # src_col = next((c for c in ["OrigineleNaam", "Frans"] if c in df.columns), df.columns[0])
            # dst_col = next((c for c in ["VertaaldeNaam", "Nederlands"] if c in df.columns), df.columns[1])
            
            # # Check of hij al bestaat in de lijst (update of toevoegen)
            # mask = (df[src_col].astype(str).str.strip() == str(original).strip())
            # if mask.any():
                # df.loc[mask, dst_col] = target
            # else:
                # self.df["mapping_cert"] = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        # # 3. Opslaan naar SQL (dbo.TM_NaamMapping) - DE ECHTE DATABASE
        # if hasattr(self, 'sql_training_manager') and self.sql_training_manager and self.sql_training_manager.engine:
            # try:
                # # 👇 HIER ZIT DE CORRECTIE: TM_NaamMapping
                # query = """
                # MERGE INTO dbo.TM_NaamMapping AS target
                # USING (SELECT :orig AS OrigineleNaam, :trans AS VertaaldeNaam) AS source
                # ON (target.OrigineleNaam = source.OrigineleNaam)
                # WHEN MATCHED THEN
                    # UPDATE SET VertaaldeNaam = source.VertaaldeNaam
                # WHEN NOT MATCHED THEN
                    # INSERT (OrigineleNaam, VertaaldeNaam, Bron)
                    # VALUES (source.OrigineleNaam, source.VertaaldeNaam, :src);
                # """
                # with self.sql_training_manager.engine.connect() as conn:
                    # conn.execute(text(query), {"orig": original, "trans": target, "src": source})
                    # conn.commit()
                # print("✅ Mapping succesvol opgeslagen in SQL (TM_NaamMapping).")
                
            # except Exception as e:
                # print(f"⚠️ Kon mapping niet naar SQL sturen: {e}")
        # else:
             # print("⚠️ Geen SQL connectie beschikbaar via sql_training_manager.")
    
    # def add_cert_mapping(self, original:  str, target: str, source: str = "Manual"):
        # """
        # Voegt een mapping toe aan dbo.TM_NaamMapping en update direct het geheugen.
        # """
        # from sqlalchemy import text
        # import pandas as pd

        # print(f"🔗 add_cert_mapping: '{original}' -> '{target}'")
        
        # # 1.Update direct het Vertaalwoordenboek (Snelheid voor normalisatie)
        # if hasattr(self, "translation_dict"):
            # self.translation_dict[original.strip()] = target.strip()

        # # 2.Update de DataFrame (Zodat je het direct in de UI lijst ziet)
        # df = self.df.get("mapping_cert", pd.DataFrame())
        # new_row = {"OrigineleNaam": original, "VertaaldeNaam": target}

        # if df.empty:
            # self.df["mapping_cert"] = pd.DataFrame([new_row])
        # else:
            # # Kolommen zoeken (flexibel)
            # src_col = next((c for c in ["OrigineleNaam", "Frans"] if c in df.columns), df.columns[0])
            # dst_col = next((c for c in ["VertaaldeNaam", "Nederlands"] if c in df.columns), df.columns[1])
            
            # # Check of hij al bestaat in de lijst (update of toevoegen)
            # mask = (df[src_col].astype(str).str.strip() == str(original).strip())
            # if mask.any():
                # df.loc[mask, dst_col] = target
            # else:
                # self.df["mapping_cert"] = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        # # 3.Opslaan naar SQL (dbo.TM_NaamMapping)
        # if hasattr(self, 'sql_training_manager') and self.sql_training_manager and self.sql_training_manager.engine:
            # try:
                # # ✅ FIX: Alleen OrigineleNaam en VertaaldeNaam (geen Bron)
                # query = """
                # MERGE INTO dbo.TM_NaamMapping AS target
                # USING (SELECT : orig AS OrigineleNaam, : trans AS VertaaldeNaam) AS source
                # ON (target.OrigineleNaam = source.OrigineleNaam)
                # WHEN MATCHED THEN
                    # UPDATE SET VertaaldeNaam = source.VertaaldeNaam
                # WHEN NOT MATCHED THEN
                    # INSERT (OrigineleNaam, VertaaldeNaam)
                    # VALUES (source.OrigineleNaam, source.VertaaldeNaam);
                # """
                # with self.sql_training_manager.engine.begin() as conn:
                    # conn.execute(text(query), {"orig": original, "trans": target})
                # print("✅ Mapping succesvol opgeslagen in SQL (TM_NaamMapping).")
                # return True
                
            # except Exception as e: 
                # print(f"⚠️ Kon mapping niet naar SQL sturen: {e}")
                # import traceback
                # traceback.print_exc()
                # return False
        # else:
            # print("⚠️ Geen SQL connectie beschikbaar via sql_training_manager.")
            # return False
    def add_cert_mapping(self, original: str, target: str, source: str = "Manual"):
        """
        Voegt een mapping toe aan dbo.TM_NaamMapping en update direct het geheugen.
        Robuuste versie: eerst UPDATE, als geen rij affected -> INSERT.
        """
        from sqlalchemy import text
        import pandas as pd

        print(f"🔗 add_cert_mapping (NEW): '{original}' -> '{target}'")

        # 1.Update direct het Vertaalwoordenboek (Snelheid voor normalisatie)
        if hasattr(self, "translation_dict"):
            self.translation_dict[original.strip()] = target.strip()

        # 2.Update de DataFrame (Zodat je het direct in de UI lijst ziet)
        df = self.df.get("mapping_cert", pd.DataFrame())
        new_row = {"OrigineleNaam": original, "VertaaldeNaam": target}

        if df.empty:
            self.df["mapping_cert"] = pd.DataFrame([new_row])
        else:
            # Kolommen zoeken (flexibel)
            src_col = next((c for c in ["OrigineleNaam", "Frans"] if c in df.columns), df.columns[0])
            dst_col = next((c for c in ["VertaaldeNaam", "Nederlands"] if c in df.columns), df.columns[1])

            # Check of hij al bestaat in de lijst (update of toevoegen)
            mask = (df[src_col].astype(str).str.strip() == str(original).strip())
            if mask.any():
                df.loc[mask, dst_col] = target
                self.df["mapping_cert"] = df
            else:
                self.df["mapping_cert"] = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        # 3.Opslaan naar SQL (dbo.TM_NaamMapping) - veilige UPDATE -> INSERT
        if hasattr(self, 'sql_training_manager') and self.sql_training_manager and getattr(self.sql_training_manager, "engine", None):
            try:
                with self.sql_training_manager.engine.begin() as conn:
                    update_sql = text("""
                        UPDATE dbo.TM_NaamMapping
                        SET VertaaldeNaam = :trans
                        WHERE OrigineleNaam = :orig
                    """)
                    res = conn.execute(update_sql, {"orig": original, "trans": target})
                    affected = getattr(res, "rowcount", None)
                    if not affected or int(affected) == 0:
                        insert_sql = text("""
                            INSERT INTO dbo.TM_NaamMapping (OrigineleNaam, VertaaldeNaam)
                            VALUES (:orig, :trans)
                        """)
                        conn.execute(insert_sql, {"orig": original, "trans": target})
                print("✅ Mapping succesvol opgeslagen in SQL (TM_NaamMapping).")
                return True
            except Exception as e:
                print(f"⚠️ Kon mapping niet naar SQL sturen: {e}")
                import traceback
                traceback.print_exc()
                return False
        else:
            print("⚠️ Geen SQL connectie beschikbaar via sql_training_manager.")
            return False
    
    def update_config_nodig(self, staff_id, cert_norm, nodig=True):
        """
        Zet 'Nodig' op True/False (wordt gebruikt door 'Activeren' knop).
        """
        import pandas as pd
        df = self.df.get("config_cert", pd.DataFrame())
        if df.empty: return

        # Zoek op ID en Genormaliseerde naam
        mask = (df["staffGID"].astype(str) == str(staff_id)) & \
               (df["CertName_norm"].astype(str) == str(cert_norm))
               
        if mask.any():
            df.loc[mask, "Nodig"] = nodig
            df.loc[mask, "LaatsteWijziging"] = pd.Timestamp.now()
            self.df["config_cert"] = df
            print(f"✅ Config geüpdatet: {staff_id} -> {cert_norm} = {nodig}")
            
            if hasattr(self, "save_config"):
                self.save_config()
            elif self.sql:
                self.sql.save_medewerker_certificaat_config(df)
        else:
            print("⚠️ Config niet gevonden om te updaten.")

    def update_sql_config_with_excel_data(self, best_info):
        """
        V24: FORCEER SYNC (DEFINITIEF).
        Schrijft Excel-datums direct naar SQL Config tabel.
        Geen filters, maar een directe push van Excel naar SQL voor 100% data-integriteit.
        """
        if not self.engine or not best_info:
            return

        print("   📥 Forceer synchronisatie: Excel datums naar SQL Config pushen...")
        updates = []
        
        # We lopen door de Excel-data (best_info) die we in load_all hebben opgebouwd
        for (sid, cnorm), info in best_info.items():
            # We verwerken alleen records waar we in de Excel een datum hebben gevonden
            if pd.notna(info["date"]) or pd.notna(info["valid"]):
                updates.append({
                    "behaald": info["date"] if pd.notna(info["date"]) else None,
                    "geldig": info["valid"] if pd.notna(info["valid"]) else None,
                    "sid": str(sid).strip(),
                    "norm": str(cnorm).strip()
                })

        if updates:
            from sqlalchemy import text
            try:
                # We updaten BehaaldOp en GeldigTot. 
                # De WHERE clause zorgt dat we alleen de medewerker + het specifieke certificaat raken.
                query = text("""
                    UPDATE dbo.TM_MedewerkerCertificaatConfig
                    SET BehaaldOp = :behaald, 
                        GeldigTot = :geldig,
                        LaatsteWijziging = GETDATE()
                    WHERE staffGID = :sid AND CertName_norm = :norm
                """)
                
                with self.engine.begin() as conn:
                    conn.execute(query, updates)
                print(f"   ✅ SQL-database succesvol gesynchroniseerd met {len(updates)} Excel-datums.")
            except Exception as e:
                print(f"   ⚠️ SQL Update fout: {e}")
        else:
            print("   ℹ️ Geen nieuwe datums gevonden in Excel om naar SQL te schrijven.")

        
    def close_tasks_for_inactive_staff(self) -> int:
        """
        Sluit alle openstaande taken af voor medewerkers die in dbo.tblSTAFF 
        niet meer op status 1 (Actief) staan. Directe SQL uitvoering.
        """
        if not self.sql_training_manager:
            return 0

        print("   🔍 SQL check op inactieve medewerkers...")
        try:
            # We voeren een directe UPDATE uit op de SQL server
            # We zoeken alle taken in de TodoPlanner waarvan de GID in tblSTAFF status <> 1 heeft
            query = text("""
                UPDATE todo
                SET 
                    todo.Status = 'Afgewerkt',
                    todo.Status_Detail = 'Automatisch afgesloten: Medewerker uit dienst (Status 2)',
                    todo.LastUpdatedAt = GETDATE()
                FROM dbo.TM_TodoPlanner todo
                INNER JOIN dbo.tblSTAFF s ON todo.staffGID = s.staffGID
                WHERE s.staffSTAFFSTATUSID <> 1
                AND todo.Status NOT IN ('Afgewerkt', 'Geweigerd')
            """)
            
            with self.engine.begin() as conn:
                result = conn.execute(query)
                modified_count = result.rowcount
            
            if modified_count > 0:
                # Belangrijk: we moeten de lokale data verversen zodat de tool het ook weet
                self.load_todo_planner() 
                return modified_count
            return 0
            
        except Exception as e:
            print(f"   ⚠️ SQL Fout bij opschonen inactieven: {e}")
            return 0
            
    def cancel_and_deactivate_task(self, task_row, deactivate_config=False) -> bool:
        """Annuleert taak in Planner en zet optioneel 'Nodig' op 0 in Config."""
        if not self.engine: return False

        task_id = task_row.get("TaskID")
        gid = task_row.get("staffGID")
        cert_norm = task_row.get("CertName_norm")
        task_type = task_row.get("TaskType") 

        # Bepaal welke config tabel we moeten aanpassen
        config_table = "dbo.TM_MedewerkerCertificaatConfig" if task_type == "Certificaat" else "dbo.TM_MedewerkerCompetentieConfig"

        try:
            with self.engine.begin() as conn:
                # 1. Update de Planner: Zet op Afgewerkt
                conn.execute(text("""
                    UPDATE dbo.TM_TodoPlanner
                    SET Status = 'Afgewerkt',
                        Status_Detail = :detail,
                        Ingeschreven_Datum = NULL,
                        Ingeschreven_Locatie = NULL,
                        LastUpdatedAt = GETDATE()
                    WHERE TaskID = :tid
                """), {"tid": task_id, "detail": "Geannuleerd en gedeactiveerd via Planner"})

                # 2. Update de Config indien aangevinkt: Zet Nodig op 0
                if deactivate_config:
                    conn.execute(text(f"""
                        UPDATE {config_table} SET Nodig = 0, LaatsteWijziging = GETDATE()
                        WHERE staffGID = :gid AND CertName_norm = :norm
                    """), {"gid": gid, "norm": cert_norm})
            
            self.load_todo_planner() # Ververs lokale data
            return True
        except Exception as e:
            print(f"⚠️ SQL Fout bij annulatie: {e}")
            return False
            
    # def save_medewerker_certificaat_config(self, df: pd.DataFrame):
        # """
        # Slaat certificaat configuratie op naar SQL. 
        # """
        # if df is None or df.empty:
            # print("   ⚠️ Geen certificaat config data om op te slaan.")
            # return False
        
        # print(f"   💾 Opslaan {len(df)} certificaat config rijen naar SQL...")
        
        # try:
            # # Verwijder kolommen die niet in SQL schema zitten
            # cols_to_drop = ["_SrcRowId", "_rn", "CertName_display"]
            # for col in cols_to_drop:
                # if col in df. columns:
                    # df = df.drop(columns=[col])
            
            # # SQL tabel naam
            # table_name = "TM_MedewerkerCertificaatConfig"
            
            # # Bestaande records voor deze medewerker(s) ophalen
            # staff_gids = df["staffGID"].unique().tolist()
            
            # for gid in staff_gids:
                # gid_data = df[df["staffGID"] == gid]. copy()
                
                # for _, row in gid_data.iterrows():
                    # cert_norm = row. get("CertName_norm", "")
                    
                    # # Check of record al bestaat
                    # check_query = f"""
                        # SELECT ConfigID FROM {table_name}
                        # WHERE staffGID = ?  AND CertName_norm = ?
                    # """
                    # existing = pd.read_sql(check_query, self.sql_engine, params=[gid, cert_norm])
                    
                    # if not existing.empty:
                        # # UPDATE bestaand record
                        # config_id = existing. iloc[0]["ConfigID"]
                        # update_query = f"""
                            # UPDATE {table_name}
                            # SET Nodig = ?,
                                # Opmerking = ?,
                                # Strategisch = ?,
                                # LaatsteWijziging = ?,
                                # GewijzigdDoor = ? 
                            # WHERE ConfigID = ?
                        # """
                        # with self.sql_engine.connect() as conn:
                            # conn.execute(
                                # update_query,
                                # [
                                    # 1 if row.get("Nodig") else 0,
                                    # row.get("Opmerking", ""),
                                    # 1 if row.get("Strategisch") else 0,
                                    # row.get("LaatsteWijziging"),
                                    # row.get("GewijzigdDoor", ""),
                                    # config_id
                                # ]
                            # )
                            # conn.commit()
                    # else:
                        # # INSERT nieuw record
                        # row_dict = row.to_dict()
                        # row_dict["Nodig"] = 1 if row_dict.get("Nodig") else 0
                        # row_dict["Strategisch"] = 1 if row_dict.get("Strategisch") else 0
                        
                        # insert_df = pd.DataFrame([row_dict])
                        # insert_df.to_sql(table_name, self.sql_engine, if_exists="append", index=False)
            
            # print(f"   ✅ Certificaat config opgeslagen voor {len(staff_gids)} medewerker(s).")
            # return True
            
        # except Exception as e:
            # print(f"   ❌ Fout bij opslaan certificaat config: {e}")
            # return False


    # def save_medewerker_competentie_config(self, df: pd. DataFrame):
        # """
        # Slaat competentie/vaardigheid configuratie op naar SQL.
        # """
        # if df is None or df. empty:
            # print("   ⚠️ Geen competentie config data om op te slaan.")
            # return False
        
        # print(f"   💾 Opslaan {len(df)} competentie config rijen naar SQL...")
        
        # try:
            # # Verwijder kolommen die niet in SQL schema zitten
            # cols_to_drop = ["_SrcRowId", "_rn", "Strategisch", "CertName", "CertName_norm", "Commentaar"]
            # for col in cols_to_drop:
                # if col in df. columns:
                    # df = df.drop(columns=[col])
            
            # # SQL tabel naam
            # table_name = "TM_MedewerkerCompetentieConfig"
            
            # # Bestaande records voor deze medewerker(s) ophalen
            # staff_gids = df["staffGID"].unique().tolist()
            
            # for gid in staff_gids: 
                # gid_data = df[df["staffGID"] == gid].copy()
                
                # for _, row in gid_data.iterrows():
                    # comp_norm = row. get("Competence_norm", "")
                    
                    # # Check of record al bestaat
                    # check_query = f"""
                        # SELECT ConfigID FROM {table_name}
                        # WHERE staffGID = ? AND Competence_norm = ?
                    # """
                    # existing = pd.read_sql(check_query, self.sql_engine, params=[gid, comp_norm])
                    
                    # if not existing.empty:
                        # # UPDATE bestaand record
                        # config_id = existing.iloc[0]["ConfigID"]
                        # update_query = f"""
                            # UPDATE {table_name}
                            # SET Nodig = ?,
                                # Opmerking = ?,
                                # LaatsteWijziging = ?,
                                # GewijzigdDoor = ? 
                            # WHERE ConfigID = ?
                        # """
                        # with self.sql_engine.connect() as conn:
                            # conn.execute(
                                # update_query,
                                # [
                                    # 1 if row.get("Nodig") else 0,
                                    # row.get("Opmerking", ""),
                                    # row.get("LaatsteWijziging"),
                                    # row.get("GewijzigdDoor", ""),
                                    # config_id
                                # ]
                            # )
                            # conn.commit()
                    # else:
                        # # INSERT nieuw record
                        # row_dict = row. to_dict()
                        # row_dict["Nodig"] = 1 if row_dict.get("Nodig") else 0
                        
                        # insert_df = pd. DataFrame([row_dict])
                        # insert_df.to_sql(table_name, self.sql_engine, if_exists="append", index=False)
            
            # print(f"   ✅ Competentie config opgeslagen voor {len(staff_gids)} medewerker(s).")
            # return True
            
        # except Exception as e:
            # print(f"   ❌ Fout bij opslaan competentie config: {e}")
            # return False
            
    def save_medewerker_certificaat_config(self, df:  pd.DataFrame):
        """Doorverwijzing naar training_manager."""
        if not self.sql_training_manager:
            print("   ❌ SQL Training Manager niet beschikbaar")
            return False
        return self.sql_training_manager. save_medewerker_certificaat_config(df)


    def save_medewerker_competentie_config(self, df: pd.DataFrame):
        """Doorverwijzing naar training_manager."""
        if not self. sql_training_manager:
            print("   ❌ SQL Training Manager niet beschikbaar")
            return False
        return self. sql_training_manager.save_medewerker_competentie_config(df)