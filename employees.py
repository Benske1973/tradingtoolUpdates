# ===============================================================
# BESTAND: xaurum/ui/tabs/employees.py
# ===============================================================

import pandas as pd
from datetime import datetime
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox, 
    QLineEdit, QScrollArea, QPushButton, QMessageBox,
    QButtonGroup, QRadioButton
)
from PyQt6.QtCore import Qt, QTimer

from xaurum.core.datastore import DataStore
# Zorg dat InfoDialog en ConfirmationDialog in widgets.py staan!
from xaurum.ui.widgets import (
    CertificateRowWidget, SearchLineEdit, SearchResultsList, 
    ToggleSwitch, ConfirmationDialog, InfoDialog
)
from xaurum.utils import normalize_certname, format_medewerker_naam
from xaurum.config import USERNAME

class EmployeeManagementTab(QWidget):
    def __init__(self, data: DataStore):
        super().__init__()
        self.data = data
        self.current_emp_id = None
        self.cert_widgets = [] 
        self.dirty = False
        
        self._last_emp_index = -1
        self._last_dept_index = -1

        self.init_ui()
    def init_ui(self):
        """Bouwt de interface op."""
        from PyQt6.QtWidgets import QCompleter
        from PyQt6.QtCore import Qt
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # ══════════════════════════════════════════════════════════════
        # 1. HEADER
        # ══════════════════════════════════════════════════════════════
        header_container = QWidget()
        header_container.setStyleSheet("background-color: white; border-bottom: 1px solid #e2e8f0;")
        header_layout = QVBoxLayout(header_container)
        header_layout.setContentsMargins(30, 20, 30, 20)
        header_layout.setSpacing(15)

        lbl_title = QLabel("👤 Medewerkerbeheer")
        lbl_title.setStyleSheet("font-family: 'Segoe UI'; font-size: 24px; font-weight: bold; color:  #002439;")
        header_layout.addWidget(lbl_title)

        filters_row = QHBoxLayout()
        filters_row.setSpacing(20)

        # Afdeling Filter
        cc_layout = QVBoxLayout()
        cc_layout.setSpacing(5)
        lbl_cc = QLabel("Afdeling (Cost Center):")
        lbl_cc. setStyleSheet("font-family: 'Segoe UI'; font-weight: bold; color:  #475569;")
        cc_layout.addWidget(lbl_cc)

        self.combo_dept = QComboBox()
        self.combo_dept.setMinimumWidth(350)
        self.combo_dept.setFixedHeight(35)
        self.combo_dept.setStyleSheet("""
            QComboBox {
                border: 1px solid #cbd5e1;
                border-radius: 0px;
                padding:  5px 10px;
                background: white;
                color: #333;
                font-family: 'Segoe UI';
            }
            QComboBox::drop-down {
                border: none;
                width: 30px;
                background: #f1f5f9;
            }
        """)
        self.combo_dept. currentIndexChanged.connect(self.on_dept_changed)
        cc_layout.addWidget(self.combo_dept)
        filters_row.addLayout(cc_layout)

        # Medewerker Filter - V2:  ZOEKBAAR MET AUTOCOMPLETE
        emp_layout = QVBoxLayout()
        emp_layout.setSpacing(5)
        lbl_emp = QLabel("Medewerker:")
        lbl_emp.setStyleSheet("font-family:  'Segoe UI'; font-weight: bold; color:  #475569;")
        emp_layout.addWidget(lbl_emp)

        self.combo_employee = QComboBox()
        self.combo_employee.setMinimumWidth(350)
        self.combo_employee.setFixedHeight(35)
        
        # ✨ NIEUWE CODE:  Maak combobox zoekbaar
        self.combo_employee. setEditable(True)
        self.combo_employee.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        
        # Styling voor editable combobox
        self.combo_employee.setStyleSheet("""
            QComboBox {
                border: 1px solid #cbd5e1;
                border-radius: 0px;
                padding: 5px 10px;
                background: white;
                color: #333;
                font-family: 'Segoe UI';
            }
            QComboBox:: drop-down {
                border: none;
                width: 30px;
                background: #f1f5f9;
            }
            QComboBox: editable {
                background: white;
            }
            QComboBox QLineEdit {
                background: white;
                border: none;
                padding: 0px;
                font-family: 'Segoe UI';
            }
        """)
        
        # ✨ Autocomplete instellen
        self.employee_completer = QCompleter()
        self.employee_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.employee_completer.setFilterMode(Qt. MatchFlag.MatchContains)
        self.combo_employee.setCompleter(self.employee_completer)
        
        # Connect:  gebruik textActivated voor editable combobox
        self.combo_employee.activated.connect(self.on_employee_changed)
        # Extra:  ook als gebruiker Enter drukt na typen
        self.combo_employee.lineEdit().returnPressed.connect(self.on_employee_search_enter)
        
        emp_layout.addWidget(self.combo_employee)
        filters_row.addLayout(emp_layout)

        filters_row.addStretch()
        
        # Info Label
        self.info_label = QLabel("")
        self.info_label.setStyleSheet("color:#64748b; font-family:'Segoe UI'; font-size:  12px;")
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignBottom)
        filters_row.addWidget(self.info_label)

        header_layout.addLayout(filters_row)
        
        # Opties rij
        options_row = QHBoxLayout()
        options_row.addStretch()
        
        lbl_nodig = QLabel("Toon 'Nodig' schakelaars:")
        lbl_nodig.setStyleSheet("font-family: 'Segoe UI'; font-size: 12px; color:#475569;")
        options_row.addWidget(lbl_nodig)

        self.toggle_show_nodig = ToggleSwitch(checked=False)
        self.toggle_show_nodig.toggled.connect(self.on_toggle_show_nodig)
        options_row.addWidget(self.toggle_show_nodig)
        
        header_layout.addLayout(options_row)
        layout.addWidget(header_container)

        # ══════════════════════════════════════════════════════════════
        # 2. SCROLL AREA
        # ══════════════════════════════════════════════════════════════
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setStyleSheet("QScrollArea { border:  none; background: #f8fafc; }")
        
        self.cert_container = QWidget()
        self.cert_container.setStyleSheet("background:  #f8fafc;")
        self.cert_layout = QVBoxLayout(self. cert_container)
        self.cert_layout.setContentsMargins(30, 20, 30, 20)
        self.cert_layout.setSpacing(0)
        self.cert_layout. setAlignment(Qt.AlignmentFlag.AlignTop)

        self.scroll.setWidget(self.cert_container)
        layout.addWidget(self.scroll, 1)

        # ══════════════════════════════════════════════════════════════
        # 3. FOOTER
        # ══════════════════════════════════════════════════════════════
        footer = QWidget()
        footer.setStyleSheet("background:  white; border-top: 1px solid #e2e8f0;")
        footer_layout = QHBoxLayout(footer)
        footer_layout.setContentsMargins(30, 15, 30, 15)
        footer_layout.setSpacing(20)

        # Type Selectie
        self.radio_cert = QRadioButton("Certificaat")
        self.radio_cert.setChecked(True)
        self.radio_comp = QRadioButton("Vaardigheid")
        
        rb_style = """
            QRadioButton { font-family: 'Segoe UI'; color: #333; font-weight: 500; }
            QRadioButton:: indicator { width: 14px; height:  14px; border-radius: 7px; border: 1px solid #cbd5e1; }
            QRadioButton::indicator:checked { background-color:  #005EB8; border: 2px solid #005EB8; }
        """
        self.radio_cert. setStyleSheet(rb_style)
        self.radio_comp.setStyleSheet(rb_style)
        
        self. type_group = QButtonGroup(self)
        self.type_group.addButton(self.radio_cert)
        self.type_group. addButton(self. radio_comp)
        self.type_group.buttonClicked.connect(self. on_type_changed)

        footer_layout.addWidget(QLabel("Toevoegen:"))
        footer_layout.addWidget(self.radio_cert)
        footer_layout.addWidget(self.radio_comp)

        # Zoekscherm
        self.search_box = SearchLineEdit(None)
        self.search_box. setPlaceholderText("🔍 Zoek certificaat of vaardigheid...")
        self.search_box. setMinimumWidth(400)
        self.search_box.setFixedHeight(40)
        self.search_box.setStyleSheet("""
            QLineEdit {
                border: 1px solid #cbd5e1;
                border-radius: 0px;
                padding:  0 15px;
                font-family: 'Segoe UI';
                font-size:  13px;
                background: #f8fafc;
            }
            QLineEdit:focus {
                border: 1px solid #005EB8;
                background: white;
            }
        """)
        self.search_box. textChanged.connect(self.on_search_changed)
        self.search_box.returnPressed.connect(self.on_search_enter)
        footer_layout. addWidget(self. search_box)

        footer_layout. addStretch()

        # Opslaan Knop
        self.btn_save = QPushButton("WIJZIGINGEN OPSLAAN")
        self.btn_save.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_save.setFixedSize(220, 45)
        self.btn_save. setStyleSheet("""
            QPushButton {
                background-color: #70bd95;
                color: white;
                font-weight: bold;
                font-family: 'Segoe UI';
                font-size:  13px;
                border: none;
                border-radius: 0px;
            }
            QPushButton:hover {
                background-color: #5da683;
            }
            QPushButton:pressed {
                background-color: #4b8a6a;
            }
        """)
        self.btn_save. clicked.connect(lambda checked: self.on_save())
        footer_layout.addWidget(self.btn_save)

        layout.addWidget(footer)

        self.results_list = SearchResultsList(self, self.search_box)
        self.results_list.hide()
        self.results_list.itemDoubleClicked. connect(lambda item:  self.add_item_from_search(item))
        self.search_box. results_list = self.results_list

    def refresh(self):
        """Ververst de dropdowns."""
        if "staff" not in self.data. df or self.data.df["staff"]. empty:
            return
        
        staff = self.data. df["staff"]
        
        # V2-FIX: Zoek CostCenter kolom in beide mogelijke namen
        cc_col = None
        for c in ("CostCenter", "staffCOSTCENTER315"):
            if c in staff.columns:
                cc_col = c
                break
        
        org_col = "staffORGUNIT"

        self.combo_dept.blockSignals(True)
        self.combo_dept.clear()

        if cc_col and cc_col in staff.columns:
            self.combo_dept.addItem("Alle afdelingen", userData=None)
            tmp = staff. copy()
            tmp[cc_col] = tmp[cc_col].astype(str)
            
            if org_col in tmp.columns:
                tmp[org_col] = tmp[org_col]. astype(str)
            else: 
                org_col = None

            for cc in sorted(tmp[cc_col].dropna().unique().tolist()):
                if cc.lower() == 'nan': 
                    continue
                
                if org_col:
                    units = tmp[tmp[cc_col] == cc][org_col].dropna().unique().tolist()
                    unit = units[0] if units else ""
                    text = f"{cc} – {unit}" if unit else cc
                else:
                    text = cc
                self. combo_dept.addItem(text, userData=cc)

            active_cc = self.data.active_costcenter
            if active_cc is not None:
                index = self.combo_dept.findData(active_cc)
                if index >= 0:
                    self.combo_dept.setCurrentIndex(index)
            else:
                self. combo_dept.setCurrentIndex(0)

        self.combo_dept.blockSignals(False)
        self.on_dept_changed()
        self._last_dept_index = self.combo_dept. currentIndex()

    def on_dept_changed(self):
        if self.current_emp_id is not None and self.dirty:
            if not self.confirm_discard_changes():
                self.combo_dept.blockSignals(True)
                self.combo_dept.setCurrentIndex(self._last_dept_index)
                self.combo_dept.blockSignals(False)
                return
            self.dirty = False

        new_index = self.combo_dept.currentIndex()
        self.combo_employee. clear()
        self.current_emp_id = None
        self.clear_cert_widgets()
        self.info_label.setText("")

        if "staff" not in self.data.df:
            return
        
        staff = self.data.df["staff"]
        selected_cc = self.combo_dept.currentData()
        
        # V2-FIX: Zoek CostCenter kolom in beide mogelijke namen
        cc_col = None
        for c in ("CostCenter", "staffCOSTCENTER315"):
            if c in staff.columns:
                cc_col = c
                break

        if selected_cc and cc_col and cc_col in staff.columns:
            subset = staff[staff[cc_col].astype(str) == str(selected_cc)]
        else:
            subset = staff

        if not subset.empty and "FullName" in subset.columns:
            names = sorted(subset["FullName"].dropna().astype(str).unique().tolist())
            self.combo_employee.addItems(names)
            
            # ✨ NIEUW: Update de completer met de namen voor autocomplete
            from PyQt6.QtCore import QStringListModel
            self.employee_completer.setModel(QStringListModel(names))
            
            # 🔧 FIX: Laad automatisch de eerste medewerker
            if names:
                self.combo_employee.setCurrentIndex(0)
                self.on_employee_changed()  # <-- DEZE REGEL TOEVOEGEN! 

        self.data.active_costcenter = selected_cc
        self._last_dept_index = new_index
        self._last_emp_index = self.combo_employee.currentIndex()
    
    def on_employee_changed(self):
        new_index = self.combo_employee.currentIndex()
        new_name = self.combo_employee.currentText()
        if new_index < 0 or not new_name:
            return

        if self.current_emp_id is not None and self.dirty:
            if not self.confirm_discard_changes():
                self.combo_employee.blockSignals(True)
                self.combo_employee.setCurrentIndex(self._last_emp_index)
                self.combo_employee.blockSignals(False)
                return
            self.dirty = False

        staff = self.data.df.get("staff")
        id_col = self.data.get_id_column()
        
        if staff is None or staff.empty or not id_col:
            return

        emp_row = staff[staff["FullName"] == new_name]
        if emp_row.empty:
            return

        self.current_emp_id = str(emp_row.iloc[0][id_col]).strip()
        self.info_label.setText(f"ID: {self.current_emp_id}")
        
        self.load_certificates_for_employee()
        self._last_emp_index = new_index

    def confirm_discard_changes(self) -> bool:
        # Gebruik de nieuwe ConfirmationDialog zodat knoppen zichtbaar zijn
        dlg = ConfirmationDialog(
            "Wijzigingen niet opgeslagen",
            "Je hebt wijzigingen die nog niet zijn opgeslagen.\nWil je doorgaan en wijzigingen negeren?",
            btn_ok_text="Ja, negeren",
            btn_cancel_text="Annuleren",
            is_danger=True,
            parent=self
        )
        return dlg.exec()

    def load_certificates_for_employee(self):
        """🌍 Laadt data en toont de lijst met taal-specifieke certificaatnamen."""
        if not self.current_emp_id:  return

        self.clear_cert_widgets()
        self.dirty = False

        # 🌍 Bepaal taal van deze medewerker
        employee_language = "N"  # Default: Nederlands
        staff_df = self.data.df. get("staff", pd.DataFrame())
        if not staff_df.empty and "staffGID" in staff_df.columns and "staffLANGUAGE" in staff_df.columns:
            emp_row = staff_df[staff_df["staffGID"]. astype(str).str.strip() == self.current_emp_id]
            if not emp_row.empty:
                employee_language = str(emp_row. iloc[0]. get("staffLANGUAGE", "N")).strip().upper()

        id_col = self.data.get_id_column()
        certs = self.data.df. get("certificates", pd.DataFrame())
        competences = self.data.df.get("competences", pd.DataFrame())
        training_req = self.data.df.get("training_req", pd.DataFrame())
        cfg_cert = self.data.df.get("config_cert", pd.DataFrame())
        cfg_comp = self.data.df.get("competence_config", pd.DataFrame())
        master_cert = self.data.master_cert_all
        master_comp = self.data.master_comp_all

        # 🐛 DEBUG: Print data voor deze medewerker
        print(f"\n{'='*70}")
        print(f"🔍 LOAD CERTIFICATES FOR:  {self.current_emp_id}")
        print(f"{'='*70}")

        rows = []
        seen_certs = set()  # 🔑 Stores NORMALIZED CODES

        # ══════════════════════════════════════════════════════════════
        # 1. Behaalde Certificaten (uit Excel/certificates)
        # ══════════════════════════════════════════════════════════════
        if not certs.empty and id_col in certs.columns:
            tmp = certs. copy()
            tmp[id_col] = tmp[id_col]. astype(str).str.strip()
            emp_cert = tmp[tmp[id_col] == self.current_emp_id]

            print(f"📊 Sectie 1 - Behaalde Certificaten: {len(emp_cert)} gevonden")

            for _, row in emp_cert.iterrows():
                name = str(row.get("CertName", "")).strip()
                if not name or name.lower() == "nan":  continue

                name_norm = self.data.normalize_certname(name)
                if name_norm in seen_certs:
                    print(f"  ⏭️  SKIP (duplicate): {name} → {name_norm}")
                    continue
                print(f"  ➕ ADD: {name} → {name_norm}")

                display_name = self.data.get_display_certname(name, employee_language)

                cfg_row = pd.DataFrame()
                if not cfg_cert.empty and id_col in cfg_cert.columns:
                    cfg_row = cfg_cert[
                        (cfg_cert[id_col]. astype(str).str.strip() == self.current_emp_id) &
                        (cfg_cert["CertName"].astype(str).str.strip() == name)
                    ]

                nodig = bool(cfg_row.iloc[0].get("Nodig", True)) if not cfg_row.empty else True
                comment = str(cfg_row.iloc[0].get("Commentaar", "") or "") if not cfg_row.empty else ""
                strategic = name in master_cert["CertName"].values if not master_cert. empty else False

                rows.append({
                    "name": display_name, "type": "Certificaat", "status": row. get("Status", "Onbekend"),
                    "achieved":  True, "expiry": row. get("Expiry_Date", None),
                    "strategic":  strategic, "nodig": nodig, "comment": comment,
                    "extra": "", "source": "certificates"
                })
                seen_certs. add(name_norm)

        # ══════════════════════════════════════════════════════════════
        # 2. Behaalde Competenties (uit Excel/competences)
        # ══════════════════════════════════════════════════════════════
        if not competences. empty and id_col in competences.columns:
            tmp = competences.copy()
            tmp[id_col] = tmp[id_col].astype(str).str.strip()
            emp_comp = tmp[tmp[id_col] == self.current_emp_id]

            print(f"📊 Sectie 2 - Behaalde Competenties: {len(emp_comp)} gevonden")

            for _, row in emp_comp.iterrows():
                name = str(row.get("Competence", "")).strip()
                if not name or name.lower() == "nan": continue

                name_norm = self.data. normalize_certname(name)
                if name_norm in seen_certs: 
                    print(f"  ⏭️  SKIP (duplicate): {name} → {name_norm}")
                    continue
                print(f"  ➕ ADD: {name} → {name_norm}")

                cfg_row = pd.DataFrame()
                if not cfg_comp.empty and id_col in cfg_comp.columns:
                    cfg_row = cfg_comp[
                        (cfg_comp[id_col].astype(str).str.strip() == self.current_emp_id) &
                        (cfg_comp["Competence"]. astype(str).str.strip() == name)
                    ]
                
                nodig = bool(cfg_row.iloc[0]. get("Nodig", True)) if not cfg_row.empty else True
                comment = str(cfg_row.iloc[0].get("Opmerking", "") or "") if not cfg_row.empty else ""
                strategic = name in master_comp["Competence"].values if not master_comp.empty else False
                validated = str(row.get("ValidatedAt", "") or "")

                rows.append({
                    "name": name, "type":  "Vaardigheid", "status": "Behaald",
                    "achieved":  True, "expiry": row. get("ValidUntil", None),
                    "strategic": strategic, "nodig":  nodig, "comment": comment,
                    "extra": f"Validatie: {validated}" if validated and validated. lower() != "nan" else "",
                    "source": "competences"
                })
                seen_certs.add(name_norm)

        # ══════════════════════════════════════════════════════════════
        # 3. Ingeschreven Opleidingen (Training Requests - GEEL)
        # ══════════════════════════════════════════════════════════════
        if not training_req.empty:
            tmp = training_req.copy()
            id_col_req = None
            for c in ["staffGID", "staffSAPNR", id_col]:
                if c in tmp.columns:
                    id_col_req = c
                    break
            
            if id_col_req:
                tmp[id_col_req] = tmp[id_col_req].astype(str).str.strip()
                emp_req = tmp[tmp[id_col_req] == self.current_emp_id]

                print(f"📊 Sectie 3 - Ingeschreven Opleidingen: {len(emp_req)} gevonden")

                for _, row in emp_req.iterrows():
                    name = str(row.get("CertName", "")).strip()
                    if not name or name. lower() == "nan": continue

                    name_norm = self.data.normalize_certname(name)
                    if name_norm in seen_certs:  continue

                    display_name = self.data.get_display_certname(name, employee_language)
                    sched_date = row.get("ScheduledDate", row.get("ScheduledDateParsed", None))

                    rows.append({
                        "name": display_name, "type": "Certificaat", "status": "Ingeschreven",
                        "achieved": False, "expiry": sched_date,
                        "strategic": name in master_cert["CertName"].values if not master_cert.empty else False,
                        "nodig": True,
                        "comment": "",
                        "extra": "📅 Ingeschreven in Xaurum",
                        "source": "training_req"
                    })
                    seen_certs. add(name_norm)

        # ═════════════════════════════════════════════���════════════════
        # 4. Certificaat Config Items (Vereist maar niet behaald - ROOD)
        # ══════════════════════════════════════════════════════════════
        if not cfg_cert.empty and id_col in cfg_cert.columns:
            emp_cfg = cfg_cert[cfg_cert[id_col].astype(str).str.strip() == self.current_emp_id]

            print(f"📊 Sectie 4 - Certificaat Config Items: {len(emp_cfg)} gevonden")

            for _, row in emp_cfg.iterrows():
                name = str(row.get("CertName", "")).strip()
                if not name or name.lower() == "nan": continue

                name_norm = self.data. normalize_certname(name)
                if name_norm in seen_certs:
                    print(f"  ⏭️  SKIP (duplicate): {name} → {name_norm}")
                    continue
                print(f"  ➕ ADD: {name} → {name_norm}")

                display_name = self.data.get_display_certname(name, employee_language)

                rows.append({
                    "name": display_name, "type": "Certificaat", "status": "Vereist (niet behaald)",
                    "achieved": False, "expiry": None,
                    "strategic": name in master_cert["CertName"].values if not master_cert.empty else False,
                    "nodig": bool(row.get("Nodig", True)),
                    "comment": str(row.get("Commentaar", "") or ""),
                    "extra": "Vereist in configuratie", "source": "config"
                })
                seen_certs.add(name_norm)

        # ══════════════════════════════════════════════════════════════
        # 5. ✅ NIEUW: Competentie Config Items (Vereist maar niet behaald - PAARS)
        # ══════════════════════════════════════════════════════════════
        if not cfg_comp.empty and id_col in cfg_comp. columns:
            emp_cfg_comp = cfg_comp[cfg_comp[id_col].astype(str).str.strip() == self.current_emp_id]

            print(f"📊 Sectie 5 - Competentie Config Items: {len(emp_cfg_comp)} gevonden")

            for _, row in emp_cfg_comp.iterrows():
                name = str(row.get("Competence", "")).strip()
                if not name or name.lower() == "nan": continue

                name_norm = self.data.normalize_certname(name)
                if name_norm in seen_certs: 
                    print(f"  ⏭️  SKIP (duplicate): {name} → {name_norm}")
                    continue
                print(f"  ➕ ADD: {name} → {name_norm}")

                # Check of al behaald in competences Excel
                is_achieved = False
                if not competences.empty and id_col in competences.columns:
                    achieved_check = competences[
                        (competences[id_col].astype(str).str.strip() == self.current_emp_id) &
                        (competences["Competence"]. astype(str).str.strip() == name)
                    ]
                    is_achieved = not achieved_check.empty

                strategic = name in master_comp["Competence"].values if not master_comp.empty else False

                rows.append({
                    "name": name,
                    "type": "Vaardigheid",
                    "status": "Behaald" if is_achieved else "Vereist (niet behaald)",
                    "achieved": is_achieved,
                    "expiry": row.get("GeldigTot", None),
                    "strategic":  strategic,
                    "nodig":  bool(row.get("Nodig", True)),
                    "comment": str(row.get("Opmerking", "") or ""),
                    "extra": "" if is_achieved else "Vereist in configuratie",
                    "source": "competence_config"
                })
                seen_certs.add(name_norm)

        # ══════════════════════════════════════════════════════════════
        # Sorteer en Bouw UI
        # ══════════════════════════════════════════════════════════════
        print(f"\n📊 TOTAAL: {len(rows)} certificaten/vaardigheden (uniek: {len(seen_certs)})")
        print(f"{'='*70}\n")

        rows = sorted(rows, key=lambda r: (0 if r["strategic"] else 1, 0 if r["nodig"] else 1, r["name"].lower()))

        for row in rows:
            w = CertificateRowWidget(
                cert_name=row["name"],
                task_type=row["type"],
                is_strategic=row["strategic"],
                status=row["status"],
                is_achieved=row["achieved"],
                nodig=row["nodig"],
                commentaar=row["comment"],
                expiry_date=row["expiry"],
                extra_info=row["extra"]
            )
            
            # --- STYLING LOGICA ---
            if row["source"] == "training_req": 
                w.set_background("ingeschreven", False)
            elif row["source"] == "config" and not row["achieved"]: 
                w.set_background("vereist", False)
            elif row["source"] == "competence_config" and not row["achieved"]: 
                # ✅ NIEUW:  Paarse styling voor vereiste vaardigheden
                w.set_background("vereist", False)
            elif row.get("status") == "Nieuw":
                w.set_background("nieuw", False)

            w.deleted.connect(self.on_certificate_deleted)
            w.switch_nodig.toggled.connect(self.mark_dirty)
            w.edit_comment.textChanged.connect(self. mark_dirty)
            
            if hasattr(self, "toggle_show_nodig"):
                w.set_nodig_visible(self.toggle_show_nodig.isChecked())

            self.cert_layout.addWidget(w)
            self.cert_widgets.append(w)

        self.cert_layout.addStretch()
    
    def clear_cert_widgets(self):
        for w in self.cert_widgets:
            w.setParent(None)
            w.deleteLater()
        self.cert_widgets.clear()
        
        while self.cert_layout.count():
            item = self.cert_layout.takeAt(0)
            w = item.widget()
            if w: w.deleteLater()

    def mark_dirty(self, *args):
        self.dirty = True

    def on_toggle_show_nodig(self, checked: bool):
        for w in self.cert_widgets:
            w.set_nodig_visible(checked)

    # ══════════════════════════════════════════════════════════════
    # ZOEK & TOEVOEG LOGICA
    # ══════════════════════════════════════════════════════════════

    def on_type_changed(self, button):
        self.on_search_changed(self.search_box.text())

    def on_search_changed(self, text: str):
        s = text.strip().lower()
        if not s:
            self.results_list.hide()
            return

        results = []
        for name in self.data.all_cert_names:
            if s in name.lower(): results.append(("CERT", name))
        for name in self.data.all_competence_names:
            if s in name.lower(): results.append(("VAARD", name))

        is_cert = self.radio_cert.isChecked()
        results.sort(key=lambda x: (0 if (x[0]=="CERT" and is_cert) or (x[0]=="VAARD" and not is_cert) else 1, x[1]))

        self.results_list.clear()
        from PyQt6.QtWidgets import QListWidgetItem
        for r_type, r_name in results[:50]:
            icon = "📜" if r_type == "CERT" else "🎯"
            item = QListWidgetItem(f"{icon} {r_name}")
            item.setData(Qt.ItemDataRole.UserRole, {"type": r_type, "name": r_name})
            self.results_list.addItem(item)

        if results:
            p = self.search_box.mapToGlobal(self.search_box.rect().bottomLeft())
            self.results_list.setFixedWidth(self.search_box.width())
            self.results_list.move(p)
            self.results_list.show()
            self.results_list.raise_()
        else:
            self.results_list.hide()

    def on_search_enter(self):
        if self.results_list.isVisible() and self.results_list.count() > 0:
            item = self.results_list.currentItem() or self.results_list.item(0)
            self.add_item_from_search(item)

    def add_item_from_search(self, item):
        data = item.data(Qt.ItemDataRole.UserRole)
        if not data: return
        
        name = data["name"]
        t_type = "Vaardigheid" if data["type"] == "VAARD" else "Certificaat"

        for w in self.cert_widgets:
            if w.cert_name.lower() == name.lower():
                QMessageBox.information(self, "Al aanwezig", f"'{name}' staat al in de lijst.")
                return

        w = CertificateRowWidget(
            cert_name=name, task_type=t_type,
            status="Nieuw", is_achieved=False, nodig=True,
            extra_info="➕ Nieuw toegevoegd"
        )
        w.set_background("nieuw", False)

        w.deleted.connect(self.on_certificate_deleted)
        w.switch_nodig.toggled.connect(self.mark_dirty)
        w.edit_comment.textChanged.connect(self.mark_dirty)
        w.set_nodig_visible(self.toggle_show_nodig.isChecked())

        self.cert_layout.insertWidget(0, w)
        self.cert_widgets.insert(0, w)
        self.dirty = True
        
        self.search_box.clear()
        self.results_list.hide()

    def add_certificate_to_list(self, text):
        pass

    # ══════════════════════════════════════════════════════════════
    # OPSLAAN & VERWIJDEREN
    # ══════════════════════════════════════════════════════════════

    # def on_certificate_deleted(self, cert_name):
        # if not self.current_emp_id: return
        
        # # Gebruik de nieuwe ConfirmationDialog
        # dlg = ConfirmationDialog(
            # "Verwijderen",
            # f"Weet je zeker dat je '{cert_name}' wilt verwijderen?\n\nDit verwijdert het item uit de database en sluit eventuele open taken.",
            # btn_ok_text="Ja, verwijderen",
            # is_danger=True,
            # parent=self
        # )
        
        # if dlg.exec():
            # # 1. Bepaal type
            # is_vaardigheid = False
            # for w in self.cert_widgets:
                # if w.cert_name == cert_name:
                    # is_vaardigheid = (w.task_type == "Vaardigheid")
                    # break

            # # 2. SQL DELETE
            # if self.data.USE_SQL_FOR_CONFIG and self.data.sql_training_manager:
                # try:
                    # if is_vaardigheid:
                        # self.data.sql_training_manager.delete_medewerker_competentie_config(self.current_emp_id, cert_name)
                    # else:
                        # self.data.sql_training_manager.delete_medewerker_certificaat_config(self.current_emp_id, cert_name)
                    
                    # # 3. SYNC DIRECT UITVOEREN (om taak uit todo te halen)
                    # self._run_planner_sync(force_refresh=True)

                # except Exception as e:
                    # print(f"Delete error: {e}")

            # # 4. Herlaad UI
            # QTimer.singleShot(100, self.load_certificates_for_employee)
            # self.dirty = False 
    # ══════════════════════════════════════════════════════════════
    # OPSLAAN & VERWIJDEREN
    # ══════════════════════════════════════════════════════════════

    def on_certificate_deleted(self, cert_name):
        """
        Verwijdert een certificaat of vaardigheid uit de configuratie en todo planner.
        """
        if not self.current_emp_id: 
            return
        
        # Gebruik de nieuwe ConfirmationDialog
        dlg = ConfirmationDialog(
            "Verwijderen",
            f"Weet je zeker dat je '{cert_name}' wilt verwijderen?\n\nDit verwijdert het item uit de database en sluit eventuele open taken.",
            btn_ok_text="Ja, verwijderen",
            is_danger=True,
            parent=self
        )
        
        if dlg.exec():
            print(f"\n{'='*60}")
            print(f"🗑️ DELETE gestart voor:  {cert_name}")
            print(f"   👤 Medewerker: {self.current_emp_id}")
            print(f"{'='*60}")
            
            # 1. Bepaal type (Certificaat of Vaardigheid)
            is_vaardigheid = False
            for w in self.cert_widgets:
                if w.cert_name == cert_name: 
                    is_vaardigheid = (w.task_type == "Vaardigheid")
                    break
            
            print(f"   📋 Type: {'Vaardigheid' if is_vaardigheid else 'Certificaat'}")

            # 2. SQL DELETE (Config + Todo)
            if self.data. USE_SQL_FOR_CONFIG and self. data.sql_training_manager:
                try:
                    # A. Verwijder uit Config tabel
                    if is_vaardigheid:
                        success = self.data. sql_training_manager.delete_medewerker_competentie_config(
                            self.current_emp_id, 
                            cert_name
                        )
                        print(f"   {'✅' if success else '❌'} Config competentie verwijderd")
                    else:
                        success = self.data. sql_training_manager.delete_medewerker_certificaat_config(
                            self.current_emp_id, 
                            cert_name
                        )
                        print(f"   {'✅' if success else '❌'} Config certificaat verwijderd")
                    
                    # B.  Verwijder ook de taak uit TodoPlanner
                    cert_name_norm = self.data.normalize_certname(cert_name)
                    if hasattr(self. data.sql_training_manager, 'delete_todo_task'):
                        todo_success = self. data.sql_training_manager.delete_todo_task(
                            self.current_emp_id, 
                            cert_name_norm
                        )
                        print(f"   {'✅' if todo_success else 'ℹ️'} Todo taak verwijderd")
                    else:
                        # Fallback:  verwijder via sync
                        print(f"   🔄 Todo wordt bijgewerkt via sync...")
                    
                    print(f"\n   ✅ {cert_name} volledig verwijderd")

                except Exception as e:
                    print(f"   ❌ Delete error: {e}")
                    import traceback
                    traceback. print_exc()
                    QMessageBox.warning(self, "Fout", f"Kon niet volledig verwijderen:\n{e}")

            # 3. Herlaad config uit SQL (zodat in-memory data klopt)
            self._reload_config_from_sql()

            # 4. Herlaad UI
            QTimer.singleShot(100, self.load_certificates_for_employee)
            
            # 5. Refresh Todo pagina
            main_window = self.window()
            if hasattr(main_window, "page_todo"):
                QTimer.singleShot(200, main_window.page_todo.refresh)
            
            self. dirty = False
            
            print(f"{'='*60}")
            print(f"✅ DELETE voltooid voor: {cert_name}")
            print(f"{'='*60}\n")
    
# ===============================================================
# AANPASSING IN xaurum/ui/tabs/employees.py (on_save methode)
# ===============================================================

    def _reload_config_from_sql(self):
        """Helper om de in-memory configuratie uit de DB te herladen."""
        if self.data.USE_SQL_FOR_CONFIG and self.data.sql_training_manager:
            try:
                # 1. Herlaad CERTIFICATEN
                fc = self.data.sql_training_manager.get_medewerker_certificaat_config()
                if fc is not None: 
                    self.data.df["config_cert"] = fc
                    self.data.df["config"] = fc # Update de fallback 'config'
                
                # 2. Herlaad COMPETENTIES
                fcomp = self.data.sql_training_manager.get_medewerker_competentie_config()
                if fcomp is not None: 
                    self.data.df["competence_config"] = fcomp
                
                print("✅ Config DF's in DataStore herladen vanuit SQL.")
            except Exception as e:
                print(f"⚠️ FOUT bij herladen config uit SQL: {e}")
                # We gaan door, maar de sync zal minder accuraat zijn
        
# ===============================================================
# BESTAND: xaurum/ui/tabs/employees.py (on_save methode)
# ===============================================================

    def on_save(self):
        """
        Slaat configuratie op. 
        Splitst data in Certificaten (met Strategisch) en Competenties (zonder Strategisch).
        V2:  Directe SQL opslag via DataStore (geen sql_training_manager nodig).
        """
        # 1. Validatie
        if not self. current_emp_id:  
            return
        
        print(f"\n{'='*60}")
        print(f"💾 on_save() - Opslaan gestart voor {self.current_emp_id}")
        print(f"{'='*60}")
        
        # 2. Data voorbereiden
        cfg_cert_rows = []
        cfg_comp_rows = []
        
        now = datetime.now()
        emp_name = self. combo_employee.currentText()
        medewerker_naam = format_medewerker_naam(emp_name)
        
        # SAP Nummer en CostCenter ophalen
        staff_sapnr = ""
        staff_costcenter = self.data.active_costcenter or ""
        staff_df = self.data. df. get("staff", pd.DataFrame())
        
        if not staff_df.empty and "staffGID" in staff_df.columns:
            row = staff_df[staff_df["staffGID"]. astype(str).str.strip() == self.current_emp_id]
            if not row.empty:
                if "staffSAPNR" in row. columns:
                    staff_sapnr = str(row.iloc[0]["staffSAPNR"])
                # CostCenter ophalen (check beide kolomnamen)
                for cc_col in ["CostCenter", "staffCOSTCENTER315"]:
                    if cc_col in row.columns:
                        staff_costcenter = str(row.iloc[0][cc_col]).strip()
                        break

        print(f"   👤 Medewerker:  {medewerker_naam} ({self.current_emp_id})")
        print(f"   🏢 CostCenter: {staff_costcenter}")
        print(f"   📋 Widgets te verwerken: {len(self.cert_widgets)}")

        # 3. Loop door widgets
        for w in self. cert_widgets: 
            d = w. get_data()
            raw_name = d["CertName"]
            
            # Sla lege regels over
            if not raw_name or not str(raw_name).strip():
                continue

            # Standaardiseer naam voor certificaten
            if w.task_type != "Vaardigheid":
                 raw_name = self.data.normalize_certname_to_standard(raw_name)

            # Basis data (voor beide tabellen)
            row_data = {
                "staffGID": self.current_emp_id,
                "staffSAPNR": staff_sapnr,
                "FullName": emp_name,
                "MedewerkerNaam": medewerker_naam,
                "CostCenter": staff_costcenter,
                "Nodig": d["Nodig"],
                "Opmerking":  d. get("Commentaar", ""),
                "Interval_maanden": 0,
                "LaatsteWijziging": now,
                "GewijzigdDoor": USERNAME
            }

            # 4. SPLITSING (Cruciaal voor SQL fouten)
            if w.task_type == "Vaardigheid": 
                # --- VAARDIGHEDEN ---
                row_data["Competence"] = raw_name
                row_data["Competence_norm"] = normalize_certname(raw_name)
                cfg_comp_rows. append(row_data)
                print(f"   🎯 Vaardigheid: {raw_name} (Nodig={d['Nodig']})")
            else:
                # --- CERTIFICATEN ---
                row_data["CertName"] = raw_name
                row_data["CertName_norm"] = normalize_certname(raw_name)
                row_data["Strategisch"] = d.get("Strategisch", False)
                cfg_cert_rows. append(row_data)
                print(f"   📜 Certificaat:  {raw_name} (Nodig={d['Nodig']})")

        print(f"\n   📊 Totaal: {len(cfg_cert_rows)} certificaten, {len(cfg_comp_rows)} vaardigheden")

        # 5. OPSLAAN NAAR SQL (Direct via DataStore)
        save_success = True
        
        try:
            # A. Certificaten opslaan
            if cfg_cert_rows:
                df_cert = pd.DataFrame(cfg_cert_rows)
                # Deduplicatie
                df_cert = df_cert. sort_values('Nodig', ascending=False)
                df_cert = df_cert.drop_duplicates(subset=['staffGID', 'CertName_norm'], keep='first')
                print(f"\n   💾 Opslaan {len(df_cert)} certificaten naar SQL...")
                
                success = self.data. save_medewerker_certificaat_config(df_cert)
                if not success:
                    save_success = False

            # B.  Competenties opslaan
            if cfg_comp_rows:
                df_comp = pd.DataFrame(cfg_comp_rows)
                # Deduplicatie
                df_comp = df_comp.sort_values('Nodig', ascending=False)
                df_comp = df_comp.drop_duplicates(subset=['staffGID', 'Competence_norm'], keep='first')
                print(f"\n   💾 Opslaan {len(df_comp)} vaardigheden naar SQL...")
                
                success = self. data.save_medewerker_competentie_config(df_comp)
                if not success: 
                    save_success = False

            if save_success:
                print("\n✅ Config succesvol opgeslagen naar SQL")
            else:
                print("\n⚠️ Config opslaan had problemen - check logs")
                
        except Exception as e:
            print(f"\n❌ Fout bij opslaan: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Fout", f"Kon niet opslaan naar SQL:\n{e}")
            return

        # 6. SYNC PLANNER (maak taken aan voor nieuwe config items)
        print(f"\n🔄 Planner sync starten...")
        self._run_planner_sync(force_refresh=True)
        self.dirty = False
        
        print(f"\n{'='*60}")
        print(f"✅ on_save() VOLTOOID")
        print(f"{'='*60}\n")
        
        dlg = InfoDialog("Succes", "Gegevens opgeslagen en planner bijgewerkt.", "OK", self)
        dlg. exec()
# ===============================================================
# BESTAND: xaurum/ui/tabs/employees.py (Definitieve _run_planner_sync)
# Dit is de oplossing die de Harde Filter garandeert.
# ===============================================================

    def _run_planner_sync(self, force_refresh=True):
        """Voert de sync uit om taken aan te maken/sluiten."""
        print("🔄 Start planner sync...")
        try:
            if self.data.sql_training_manager and force_refresh:
                
                # 1. Herlaad CONFIG (Master bron)
                fc = self.data.sql_training_manager.get_medewerker_certificaat_config()
                if fc is not None: 
                    self.data.df["config_cert"] = fc
                    self.data.df["config"] = fc 
                
                fcomp = self.data.sql_training_manager.get_medewerker_competentie_config()
                if fcomp is not None: 
                    self.data.df["competence_config"] = fcomp
                
                print("   -> Config DF's in DataStore herladen vanuit SQL.")
                

                # 2. Herlaad TODO (Haalt alle bestaande taken op)
                todo_df = self.data.sql_training_manager.get_todo_planner()
                if todo_df is not None:
                    self.data.df["todo"] = todo_df
                    print(f"   -> Todo herladen: {len(todo_df)} taken")
                
                todo = self.data.df.get("todo", pd.DataFrame())
                cfg_cert = self.data.df.get("config_cert", pd.DataFrame())
                cfg_comp = self.data.df.get("competence_config", pd.DataFrame())
                
                # Definitie van is_truthy (indien nodig)
                is_truthy = self.data.is_truthy_value if hasattr(self.data, 'is_truthy_value') else lambda val: str(val).lower().strip() in ('true', '1', 'y', 'ja', 'yes')

                
                # >>> KRITIEKE FIX 1: Forceer de 'Nodig' vlag op basis van de verse Config
                if not todo.empty and (not cfg_cert.empty or not cfg_comp.empty) and "CertName_norm" in todo.columns:
                    
                    # Mapping van Configuratie (Certificaten)
                    cert_map = {}
                    if "staffGID" in cfg_cert.columns and "CertName" in cfg_cert.columns:
                        cfg_cert["CertName_norm"] = cfg_cert["CertName"].astype(str).apply(self.data.normalize_certname)
                        cert_map = cfg_cert.set_index(["staffGID", "CertName_norm"])["Nodig"].to_dict()
                    
                    # Mapping van Configuratie (Vaardigheden)
                    comp_map = {}
                    if "staffGID" in cfg_comp.columns and "Competence" in cfg_comp.columns:
                        cfg_comp["CertName_norm"] = cfg_comp["Competence"].astype(str).apply(self.data.normalize_certname)
                        comp_map = cfg_comp.set_index(["staffGID", "CertName_norm"])["Nodig"].to_dict()

                    # Functie die de Nodig vlag uit de Master Config haalt
                    def check_nodig_status(row):
                        staff_id = str(row.get("staffGID", "")).strip()
                        cert_norm = str(row.get("CertName_norm", "")).strip()
                        task_type = str(row.get("TaskType", "")).strip()
                        key = (staff_id, cert_norm)
                        default_nodig = is_truthy(row.get("Nodig", True))

                        if "Certificaat" in task_type and key in cert_map:
                            return is_truthy(cert_map.get(key, default_nodig))
                        elif "Vaardigheid" in task_type and key in comp_map:
                            return is_truthy(comp_map.get(key, default_nodig))
                        return default_nodig
                    
                    todo["Nodig"] = todo.apply(check_nodig_status, axis=1)
                    self.data.df["todo"] = todo
                    print("   -> 'Nodig' vlag in Todo DF bijgewerkt naar verse Config.")
                # <<< EINDE KRITIEKE FIX 1


                # 3. SLUIT TAKEN (In-memory update: Status -> Afgewerkt)
                self.data.close_tasks_no_longer_needed()
                print("   -> Taken in geheugen gemarkeerd als Afgewerkt.")


                # >>> KRITIEKE FIX 2: BRUTE FORCE ELIMINATIE (Nodig=False moet verdwijnen)
                # We gaan de NODIG=TRUE sleutels direct uit de verse config halen.
                
                if not todo.empty:
                    before_count = len(todo)

                    # 1. Bepaal de sleutels (staffGID, CertName_norm) van ALLE taken die NODIG ZIJN
                    
                    # Certificaten Nodig=True
                    cert_nodig = cfg_cert[cfg_cert["Nodig"].apply(is_truthy)].copy()
                    cert_nodig["CertName_norm"] = cert_nodig["CertName"].astype(str).apply(self.data.normalize_certname)
                    cert_keys = cert_nodig.set_index(["staffGID", "CertName_norm"]).index.tolist()

                    # Competenties Nodig=True
                    comp_nodig = cfg_comp[cfg_comp["Nodig"].apply(is_truthy)].copy()
                    comp_nodig["CertName_norm"] = comp_nodig["Competence"].astype(str).apply(self.data.normalize_certname)
                    comp_keys = comp_nodig.set_index(["staffGID", "CertName_norm"]).index.tolist()

                    needed_keys = set(cert_keys + comp_keys)

                    # 2. Filteren: Behoud ALLEEN taken die NODIG ZIJN OF AL DEFINITIEF GESLOTEN
                    todo["key"] = list(zip(todo["staffGID"], todo["CertName_norm"]))
                    
                    is_needed = todo["key"].apply(lambda x: x in needed_keys)
                    is_closed_for_history = todo["Status"].astype(str).str.lower().isin(["afgewerkt", "gesloten", "certified"])

                    # Behoud alles wat Nodig is OF alles wat Gesloten is (dit voorkomt dat correct afgewerkte taken verdwijnen)
                    todo_to_keep = todo[is_needed | is_closed_for_history].drop(columns=["key"]).copy()
                    
                    removed_count = before_count - len(todo_to_keep)
                    if removed_count > 0:
                        self.data.df["todo"] = todo_to_keep
                        print(f"   -> {removed_count} onnodige/verouderde taken (Nodig=False uit Config) VERWIJDERD uit geheugen.")
                    else:
                        print("   -> Geen onnodige taken om te verwijderen.")
                # <<< EINDE KRITIEKE FIX 2


                # 4. KRITIEKE STAP: SLA NU DE SCHONE STATUS DIRECT OP NAAR SQL
                # Dit is de laatste stap die de taak definitief elimineert uit de database.
                if self.data.USE_SQL_FOR_TODO:
                    self.data.save_todo() 
                    print("   -> Schone takenlijst (zonder Nodig=False) opgeslagen naar SQL.")


                # 5. Sync Logic (Creëert nieuwe taken/sync inschrijvingen)
                self.data.sync_todo_with_config()
                self.data.create_tasks_for_expiring_certificates()  # 🆕 Scan certificaten op vervaldata
                self.data.sync_competence_tasks()
                self.data.update_status_from_tasktype_and_xaurum()
                self.data.apply_overrule_with_zweef()
                self.data.close_finished_tasks()
                # Detecteer afwezigen bij afgelopen opleidingen
                self.data.detect_absent_from_completed_training()

                # 6. Final Save (met staff info enrichment)
                if self.data.USE_SQL_FOR_TODO:
                    self.data.enrich_todo_with_staff_info()  # 🆕 Vul CostCenter/MedewerkerNaam in
                    self.data.save_todo()
                    print("   -> Sync resultaten opgeslagen naar SQL")

                # 7. UI Verversen
                self.load_certificates_for_employee()
                main_window = self.window()
                if hasattr(main_window, "page_todo"):
                    main_window.page_todo.refresh()

        except Exception as e:
            print(f"❌ Sync fout: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.warning(self, "Sync Fout", f"Opslaan gelukt, maar sync faalde:\n{e}")
    
    def select_employee_by_name(self, name):
        if not name: return
        idx = self.combo_employee.findText(name, Qt.MatchFlag.MatchContains)
        if idx >= 0:
            self.combo_employee.setCurrentIndex(idx)
    
    def on_employee_search_enter(self):
        """Wordt aangeroepen als gebruiker Enter drukt in het zoekveld."""
        typed_text = self. combo_employee.lineEdit().text().strip()
        if not typed_text: 
            return
        
        # Zoek exacte match in de combobox items
        index = self.combo_employee.findText(typed_text, Qt. MatchFlag. MatchExactly)
        
        if index >= 0:
            # Exacte match gevonden
            self.combo_employee.setCurrentIndex(index)
            self.on_employee_changed()
        else: 
            # Probeer case-insensitive match
            for i in range(self.combo_employee. count()):
                if self.combo_employee.itemText(i).lower() == typed_text.lower():
                    self.combo_employee.setCurrentIndex(i)
                    self.on_employee_changed()
                    return
            
            # Geen match - selecteer eerste suggestie als die er is
            index = self.combo_employee.findText(typed_text, Qt.MatchFlag.MatchContains)
            if index >= 0:
                self.combo_employee.setCurrentIndex(index)
                self.on_employee_changed()