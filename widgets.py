# ===============================================================
# BESTAND: xaurum/ui/widgets.py
# ===============================================================

import pandas as pd
import math
from typing import Optional
from datetime import datetime, timedelta

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, 
    QListWidget, QPushButton, QCheckBox, QDialog, QDialogButtonBox, 
    QComboBox, QSizePolicy, QGraphicsDropShadowEffect
)
from PyQt6.QtCore import Qt, pyqtSignal, QRectF, QPointF
from PyQt6.QtGui import (
    QPainter, QColor, QPen, QBrush, QLinearGradient, 
    QPainterPath, QRadialGradient, QPixmap
)


# ===============================================================
# 1. INFO DIALOG (VOOR SUCCES MELDINGEN)
# ===============================================================
class InfoDialog(QDialog):
    def __init__(self, title, message, btn_text="OK", parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.resize(350, 160)
        self.setStyleSheet("""
            QDialog { background-color: white; border: 1px solid #cbd5e1; }
            QLabel { font-family: 'Segoe UI'; font-size: 13px; color: #333333; }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(25, 25, 25, 25)
        layout.setSpacing(20)

        lbl_msg = QLabel(message)
        lbl_msg.setWordWrap(True)
        layout.addWidget(lbl_msg)

        layout.addStretch()

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self.btn_ok = QPushButton(btn_text)
        self.btn_ok.setCursor(Qt.CursorShape.PointingHandCursor)
        
        # FIX: Rand en min-width toegevoegd zodat de knop ALTIJD zichtbaar is
        self.btn_ok.setStyleSheet("""
            QPushButton {
                background-color: #70bd95; 
                border: 1px solid #5da683;
                color: white !important;
                padding: 8px 25px;
                font-family: 'Segoe UI';
                font-weight: bold;
                border-radius: 2px;
                min-width: 100px;
            }
            QPushButton:hover { background-color: #5da683; }
        """)
        self.btn_ok.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_ok)

        layout.addLayout(btn_layout)

# ===============================================================
# 2. CONFIRMATION DIALOG (VOOR DELETE/VRAGEN)
# ===============================================================
class ConfirmationDialog(QDialog):
    def __init__(self, title, message, btn_ok_text="Ja, verwijderen", btn_cancel_text="Annuleren", is_danger=True, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.resize(420, 180)
        self.setStyleSheet("""
            QDialog { background-color: white; border: 1px solid #cbd5e1; }
            QLabel { font-family: 'Segoe UI'; font-size: 13px; color: #333333; }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(30, 30, 30, 30)
        layout.setSpacing(20)

        lbl_msg = QLabel(message)
        lbl_msg.setWordWrap(True)
        layout.addWidget(lbl_msg)

        layout.addStretch()

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self.btn_cancel = QPushButton(btn_cancel_text)
        self.btn_cancel.setCursor(Qt.CursorShape.PointingHandCursor)
        
        # FIX: Grijze rand toegevoegd voor contrast op witte achtergrond
        self.btn_cancel.setStyleSheet("""
            QPushButton {
                background-color: #f8fafc;
                border: 1px solid #cbd5e1;
                color: #334155 !important;
                padding: 8px 16px;
                font-family: 'Segoe UI';
                border-radius: 2px;
                min-width: 100px;
            }
            QPushButton:hover { background-color: #f1f5f9; }
        """)
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_cancel)

        self.btn_ok = QPushButton(btn_ok_text)
        self.btn_ok.setCursor(Qt.CursorShape.PointingHandCursor)
        
        if is_danger:
            ok_style = """
                QPushButton {
                    background-color: #ef4444; border: 1px solid #dc2626; color: white !important;
                    padding: 8px 16px; font-family: 'Segoe UI'; font-weight: bold; border-radius: 2px;
                    min-width: 100px;
                }
                QPushButton:hover { background-color: #dc2626; }
            """
        else:
            ok_style = """
                QPushButton {
                    background-color: #70bd95; border: 1px solid #5da683; color: white !important;
                    padding: 8px 16px; font-family: 'Segoe UI'; font-weight: bold; border-radius: 2px;
                    min-width: 100px;
                }
                QPushButton:hover { background-color: #5da683; }
            """
            
        self.btn_ok.setStyleSheet(ok_style)
        self.btn_ok.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_ok)
        layout.addLayout(btn_layout)

# ===============================================================
# 3. COSTCENTER DIALOOG
# ===============================================================
class CostCenterDialog(QDialog):
    def __init__(self, staff_df: pd.DataFrame, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Kies costcenter")
        self.setModal(True)
        self.resize(400, 280)
        self.setStyleSheet("background-color: white;")

        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)

        label = QLabel("Kies voor welk costcenter je gaat werken.\nMet 'Volledig beheer' kan je alle costcenters beheren.")
        label.setWordWrap(True)
        label.setStyleSheet("font-family: 'Segoe UI'; font-size: 13px; color: #333;")
        layout.addWidget(label)

        self.combo = QComboBox()
        self.combo.setFixedHeight(35)
        self.combo.setStyleSheet("""
            QComboBox { border: 1px solid #cbd5e1; padding: 5px; font-family: 'Segoe UI'; background: #f8fafc; color: #333; } 
            QComboBox::drop-down { border: none; background: #e2e8f0; width: 30px; }
            QComboBox QAbstractItemView { background: white; border: 1px solid #cbd5e1; selection-background-color: #e0f2fe; }
        """)
        layout.addWidget(self.combo)

        self.chk_full = QCheckBox("Volledig beheer alle costcenters (geen beperking)")
        self.chk_full.setStyleSheet("font-family: 'Segoe UI'; font-size: 12px; color: #333;")
        layout.addWidget(self.chk_full)

        cc_col = "staffCOSTCENTER315"
        org_col = "staffORGUNIT" if "staffORGUNIT" in staff_df.columns else None
        self.combo.addItem("Alle afdelingen", userData=None)

        if cc_col in staff_df.columns:
            tmp = staff_df.copy(); tmp[cc_col] = tmp[cc_col].astype(str)
            if org_col: tmp[org_col] = tmp[org_col].astype(str)
            for cc in sorted(tmp[cc_col].dropna().unique().tolist()):
                text = cc
                if org_col:
                    units = tmp[tmp[cc_col] == cc][org_col].dropna().unique().tolist()
                    if units: text = f"{cc} – {units[0]}"
                self.combo.addItem(text, userData=cc)

        # FIX: QDialogButtonBox styling geforceerd voor zichtbaarheid
        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.button_box.setStyleSheet("""
            QPushButton { 
                background-color: #005EB8; 
                color: white !important; 
                border: 1px solid #004a91; 
                padding: 8px 20px; 
                font-weight: bold; 
                font-family: 'Segoe UI'; 
                min-width: 80px;
            } 
            QPushButton:hover { background-color: #004a91; }
            /* Specifieke stijl voor Cancel/Annuleren knop in de box */
            QPushButton[text="Cancel"], QPushButton[text="Annuleren"] {
                background-color: #f1f5f9;
                color: #333 !important;
                border: 1px solid #cbd5e1;
            }
        """)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

    def selected_costcenter(self): return self.combo.currentData()
    def is_full_access(self): return self.chk_full.isChecked()
# ===============================================================
# 4. SEARCH WIDGETS
# ===============================================================
class SearchLineEdit(QLineEdit):
    def __init__(self, results_list: 'SearchResultsList'):
        super().__init__()
        self.results_list = results_list
    def keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_Down:
            if self.results_list and self.results_list.isVisible() and self.results_list.count() > 0:
                self.results_list.setFocus(); self.results_list.setCurrentRow(0); return
        super().keyPressEvent(event)

class SearchResultsList(QListWidget):
    def __init__(self, parent_tab, search_box: Optional[SearchLineEdit]):
        super().__init__()
        self.parent_tab = parent_tab; self.search_box = search_box
        self.setWindowFlags(Qt.WindowType.ToolTip | Qt.WindowType.FramelessWindowHint)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating)
        self.setStyleSheet("""
            QListWidget { background-color: white; border: 1px solid #005EB8; font-family: 'Segoe UI'; } 
            QListWidget::item { padding: 8px; border-bottom: 1px solid #f1f5f9; } 
            QListWidget::item:selected { background-color: #e0f2fe; color: #005EB8; }
        """)
    def keyPressEvent(self, event):
        if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            item = self.currentItem()
            if item and hasattr(self.parent_tab, "add_certificate_to_list"): self.parent_tab.add_certificate_to_list(item.text()); return
        elif event.key() == Qt.Key.Key_Escape:
            if self.search_box: self.search_box.setFocus(); self.hide(); return
        super().keyPressEvent(event)

# ===============================================================
# 5. TOGGLE SWITCH
# ===============================================================
class ToggleSwitch(QWidget):
    toggled = pyqtSignal(bool)
    def __init__(self, checked: bool = False, parent=None):
        super().__init__(parent)
        self._checked = bool(checked); self.setCursor(Qt.CursorShape.PointingHandCursor); self.setMinimumSize(36, 18); self.setMaximumHeight(18)
    def isChecked(self): return self._checked
    def setChecked(self, value):
        if self._checked != value: self._checked = value; self.toggled.emit(self._checked); self.update()
    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton: self.setChecked(not self._checked)
    def paintEvent(self, event):
        p = QPainter(self); p.setRenderHint(QPainter.RenderHint.Antialiasing)
        rect = self.rect(); h = rect.height(); w = rect.width(); radius = h / 2
        bg = QColor("#70bd95") if self._checked else QColor("#cbd5e1")
        p.setBrush(bg); p.setPen(Qt.PenStyle.NoPen); p.drawRoundedRect(rect.adjusted(1, 1, -1, -1), radius, radius)
        margin = 2; d = h - 2 * margin; x = w - margin - d if self._checked else margin
        p.setBrush(QColor("white")); p.drawEllipse(QRectF(x, margin, d, d)); p.end()

# ===============================================================
# 6. CLICKABLE LABEL
# ===============================================================
class ClickableLabel(QLabel):
    clicked = pyqtSignal()
    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton: self.clicked.emit()
        super().mousePressEvent(event)

# ===============================================================
# 7. STAT CARD (DASHBOARD)
# ===============================================================
class StatCard(QWidget):
    def __init__(self, title: str, value: str, icon_text: str, color_hex: str):
        super().__init__()
        self.setMinimumHeight(110)
        self.setStyleSheet(f"""
            QWidget#CardContainer {{ background-color: white; border-radius: 10px; border: 1px solid #e2e8f0; }}
            QLabel#IconLabel {{ background-color: {color_hex}20; color: {color_hex}; border-radius: 24px; font-size: 20px; font-weight: bold; }}
        """)
        layout = QHBoxLayout(self); layout.setContentsMargins(0, 0, 0, 0)
        self.container = QWidget(); self.container.setObjectName("CardContainer")
        shadow = QGraphicsDropShadowEffect(); shadow.setBlurRadius(15); shadow.setYOffset(4); shadow.setColor(QColor(0, 0, 0, 15))
        self.container.setGraphicsEffect(shadow)
        
        inner = QHBoxLayout(self.container); inner.setContentsMargins(20, 20, 20, 20); inner.setSpacing(15)
        self.lbl_icon = QLabel(icon_text); self.lbl_icon.setObjectName("IconLabel"); self.lbl_icon.setFixedSize(48, 48); self.lbl_icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
        inner.addWidget(self.lbl_icon)
        
        txt_cont = QWidget(); txt_lay = QVBoxLayout(txt_cont); txt_lay.setContentsMargins(0,0,0,0); txt_lay.setSpacing(2); txt_lay.setAlignment(Qt.AlignmentFlag.AlignVCenter)
        self.lbl_title = QLabel(title); self.lbl_title.setStyleSheet("font-family: 'Segoe UI'; font-size: 13px; color: #64748b; font-weight: 600;")
        self.lbl_value = QLabel(value); self.lbl_value.setStyleSheet("font-family: 'Segoe UI'; font-size: 26px; color: #002439; font-weight: bold;"); self.lbl_value.setObjectName("card_value")
        txt_lay.addWidget(self.lbl_title); txt_lay.addWidget(self.lbl_value)
        inner.addWidget(txt_cont); inner.addStretch(); layout.addWidget(self.container)

    def update_value(self, value: str):
        self.lbl_value.setText(value)

# ===============================================================
# 8. CERTIFICATE ROW WIDGET (DE LIJST ITEMS)
# ===============================================================
class CertificateRowWidget(QWidget):
    deleted = pyqtSignal(str) 

    def __init__(self, cert_name, task_type="Certificaat", is_strategic=False, status="", is_achieved=False, nodig=True, commentaar="", expiry_date=None, extra_info=""):
        super().__init__()
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.cert_name = cert_name; self.task_type = task_type; self.is_strategic = is_strategic; self.details_visible = False

        badge_style = "background-color: #002439; color: white; font-weight: bold; padding: 0px 6px; border-radius: 0px; font-size: 10px; font-family: 'Segoe UI'; min-height: 18px; max-height: 18px;"
        left_color = "#9333ea" if task_type == "Vaardigheid" else "#005EB8"
        icon = "🎯" if task_type == "Vaardigheid" else "📜"
        
        self.setStyleSheet(f"CertificateRowWidget {{ border-left: 5px solid {left_color}; background: white; margin: 2px 0; }}")

        main = QVBoxLayout(self); main.setContentsMargins(0, 0, 0, 0); main.setSpacing(0)
        
        # Container TRANSPARANT voor achtergrondkleuren!
        container = QWidget(); container.setStyleSheet("background: transparent;") 
        clayout = QVBoxLayout(container); clayout.setContentsMargins(10, 8, 10, 8); clayout.setSpacing(4)

        top = QHBoxLayout(); top.setSpacing(10)
        self.lbl_title = ClickableLabel(f"{icon}  {cert_name}")
        self.lbl_title.setStyleSheet("font-family: 'Segoe UI'; font-size: 13px; color: #1f2937; font-weight: 600; background: transparent;")
        self.lbl_title.clicked.connect(self.toggle_details); top.addWidget(self.lbl_title, stretch=3)

        badge = QLabel(task_type); badge.setStyleSheet(badge_style); badge.setAlignment(Qt.AlignmentFlag.AlignCenter); top.addWidget(badge)

        # DELETE KNOP: Donkere kleur (#333) zodat het zichtbaar is op wit
        self.btn_del = QPushButton("🗑️")
        self.btn_del.setFixedSize(30, 30); self.btn_del.setCursor(Qt.CursorShape.PointingHandCursor); self.btn_del.setFlat(True)
        self.btn_del.setStyleSheet("""
            QPushButton { 
                border: none; 
                outline: none; 
                background: transparent; 
                color: #333333; 
                font-size: 14px; 
                text-align: center; 
            } 
            QPushButton:hover { 
                background-color: #fee2e2; 
                border-radius: 0px; 
            }
        """)
        
        self.btn_del.clicked.connect(lambda checked: self.deleted.emit(self.cert_name))
        
        top.addWidget(self.btn_del)
        clayout.addLayout(top)

        self.detail_box = QWidget(); self.detail_box.setStyleSheet("background: rgba(255,255,255,0.5); margin-top: 5px; border-radius: 0px;")
        dlay = QVBoxLayout(self.detail_box); dlay.setContentsMargins(10, 10, 10, 10); dlay.setSpacing(8)

        if expiry_date:
            try:
                dt = pd.to_datetime(expiry_date)
                txt = "Onbeperkt geldig" if dt.year >= 2099 else dt.strftime("%d-%m-%Y")
                col = "#059669" if dt.year >= 2099 else ("#dc2626" if (dt - pd.Timestamp.now()).days < 0 else "#059669")
                lbl = QLabel(f"📅 Geldig tot: <span style='color:{col};font-weight:bold;'>{txt}</span>")
                lbl.setStyleSheet("background: transparent;")
                dlay.addWidget(lbl)
            except: pass
        
        if extra_info:
            lbl = QLabel(extra_info); lbl.setStyleSheet("color: #64748b; font-size: 11px; font-family: 'Segoe UI'; font-style: italic; background: transparent;"); dlay.addWidget(lbl)

        nod_row = QHBoxLayout()
        self.lbl_nodig = QLabel("Nodig:"); self.lbl_nodig.setStyleSheet("font-size: 12px; color: #374151; font-family: 'Segoe UI'; background: transparent;")
        nod_row.addWidget(self.lbl_nodig)
        self.switch_nodig = ToggleSwitch(checked=nodig); nod_row.addWidget(self.switch_nodig); nod_row.addStretch(); dlay.addLayout(nod_row)

        com_row = QHBoxLayout()
        com_row.addWidget(QLabel("Opmerking:", styleSheet="font-size: 12px; color: #374151; font-family: 'Segoe UI'; background: transparent;"))
        self.edit_comment = QLineEdit(commentaar); self.edit_comment.setPlaceholderText("...")
        self.edit_comment.setStyleSheet("QLineEdit { border: 1px solid #cbd5e1; border-radius: 0px; padding: 4px; font-size: 12px; background: white; }")
        com_row.addWidget(self.edit_comment); dlay.addLayout(com_row)

        self.detail_box.setVisible(False); clayout.addWidget(self.detail_box); main.addWidget(container)
        
        self.set_background(status, is_achieved)

    def toggle_details(self): 
        self.details_visible = not self.details_visible
        self.detail_box.setVisible(self.details_visible)

    # FIX: Open het detailscherm als 'nodig' zichtbaar moet zijn
    def set_nodig_visible(self, visible: bool):
        self.switch_nodig.setVisible(True)
        self.details_visible = visible
        self.detail_box.setVisible(visible)

    def get_data(self): return {"CertName": self.cert_name, "Nodig": self.switch_nodig.isChecked(), "Strategisch": self.is_strategic, "Commentaar": self.edit_comment.text().strip()}
    
    def set_background(self, status: str, is_achieved: bool):
        status_lower = str(status).lower(); bg_col = "white"; border_col = "#e2e8f0"
        
        if "ingeschreven" in status_lower: bg_col = "#fffbeb"; border_col = "#d97706"
        elif "vereist" in status_lower and not is_achieved: bg_col = "#fef2f2"; border_col = "#dc2626"
        elif "verloopt" in status_lower: bg_col = "#fffbeb"; border_col = "#fde68a"
        elif "nieuw" in status_lower: bg_col = "#eff6ff"; border_col = "#bfdbfe"
        elif status_lower == "actief" or is_achieved: bg_col = "#f0fdf4"; border_col = "#bbf7d0"

        left_color = "#9333ea" if self.task_type == "Vaardigheid" else "#005EB8"
        if "ingeschreven" in status_lower: left_color = "#d97706"
        elif "vereist" in status_lower and not is_achieved: left_color = "#dc2626"

        self.setStyleSheet(f"CertificateRowWidget {{ background-color: {bg_col}; border: 1px solid {border_col}; border-left: 5px solid {left_color}; border-radius: 0px; margin: 2px 0; }}")


# # ===============================================================
# # BESTAND: xaurum/ui/widgets. py (GEWIJZIGDE TodoRowWidget)
# # ===============================================================
# class TodoRowWidget(QWidget):
    # def __init__(self, row_data:  pd.Series):
        # super().__init__()
        # self.setAttribute(Qt. WidgetAttribute.WA_StyledBackground, True)
        # self.row_data = row_data

        # # --- 1. Data ophalen ---
        # status = str(row_data. get("Status", ""))
        # detail = str(row_data.get("Status_Detail", ""))
        # medewerker = str(row_data. get("MedewerkerNaam", "-"))
        # task_type = str(row_data.get("TaskType", "")).strip()
        # sl = status.lower()
        # dl = detail.lower()
        
        # # 🔧 FIX: Robuuste CertName handling
        # def _safe_cert_name(val):
            # if pd.isna(val): return ""
            # s = str(val).strip()
            # return "" if s.lower() == "nan" else s
        
        # cert = _safe_cert_name(row_data.get("CertName_display", "")) or \
               # _safe_cert_name(row_data.get("CertName", "")) or \
               # _safe_cert_name(row_data.get("competence", "")) or "?"

        # # --- 2.  INITIALISATIE (VOORKOMT CRASH) ---
        # is_past = False
        # display_info = ""
        # icon = "⚪"
        # bg_col = "#f0fdf4"
        # left_color = "#10b981"
        # is_new_item = False  # ✅ NIEUW:  Track of het een nieuwe taak is
        
        # # --- 3.  Styling bepalen ---
        # if "niet geslaagd" in dl or "failed" in dl: 
            # icon = "⛔"
            # bg_col = "#ffcdd2"
            # left_color = "#dc2626"
            # display_info = "Herkansing nodig"
            
        # elif sl == "open":
            # exp_dt = row_data.get("ExpiryDate", pd.NaT)
            # expiry_str = self._format_expiry(exp_dt)
            
            # # ✅ Check of het een NIEUW certificaat is (nog nooit behaald)
            # if "nieuw certificaat nodig" in dl:
                # icon = "🔵"
                # bg_col = "#eff6ff"  # Lichtblauw
                # left_color = "#3b82f6"  # Blauw
                # is_new_item = True
                # # Toon NIEUW + eventuele vervaldatum
                # if pd.notna(exp_dt):
                    # display_info = f"🆕 NIEUW | Vervalt: {expiry_str}"
                # else:
                    # display_info = "🆕 NIEUW - In te plannen"
            
            # # ✅ NIEUW: Check of het een NIEUWE vaardigheid is
            # elif "vaardigheid nog niet behaald" in dl or "nieuwe vaardigheid nodig" in dl: 
                # icon = "🔵"
                # bg_col = "#eff6ff"  # Lichtblauw
                # left_color = "#3b82f6"  # Blauw
                # is_new_item = True
                # display_info = "🆕 NIEUW - In te plannen"
            
            # else:
                # # Bestaand certificaat dat verloopt/verlopen is
                # icon = "🔴"
                # bg_col = "#fff1f2"
                # left_color = "#dc2626"
                # display_info = f"Vervalt: {expiry_str}"
        
        # # elif sl in ["ingeschreven", "in wachtrij", "on hold"]:
            # # ing_date = row_data.get("Ingeschreven_Datum", pd.NaT)
            # # loc = str(row_data. get("Ingeschreven_Locatie", "")).strip().replace("nan", "")
            
            # # # Check of datum gepasseerd is
            # # is_past = pd. notna(ing_date) and pd.to_datetime(ing_date).date() < datetime.now().date()
            
            # # if is_past: 
                # # icon = "⌛"
                # # bg_col = "#f1f5f9"
                # # left_color = "#94a3b8"
                # # display_info = f"GEPASSEERD ({pd.to_datetime(ing_date).strftime('%d-%m-%Y')}) | Wacht op export"
            # # else: 
                # # icon = "🟠"
                # # bg_col = "#fff7ed"
                # # left_color = "#d97706"
                # # date_str = pd. to_datetime(ing_date).strftime('%d-%m-%Y') if pd.notna(ing_date) else "Datum onbekend"
                # # display_info = f"📅 {date_str} - 📍 {loc or 'Locatie onbekend'}"
        # # elif sl in ["ingeschreven", "in wachtrij", "on hold"]:
            # # ing_date = row_data.get("Ingeschreven_Datum", pd.NaT)
            # # loc = str(row_data. get("Ingeschreven_Locatie", "")).strip()
            # # # Clean up "nan" en "None" strings
            # # if loc. lower() in ["nan", "none", ""]:
                # # loc = ""
            
            # # # Check of datum gepasseerd is
            # # is_past = False
            # # try:
                # # is_past = pd.notna(ing_date) and pd.to_datetime(ing_date).date() < datetime.now().date()
            # # except: 
                # # pass

            # # if is_past: 
                # # icon = "⌛"
                # # bg_col = "#f1f5f9"
                # # left_color = "#94a3b8"
                # # display_info = f"GEPASSEERD ({pd.to_datetime(ing_date).strftime('%d-%m-%Y')}) | Wacht op export"
            # # else: 
                # # icon = "🟠"
                # # bg_col = "#fff7ed"
                # # left_color = "#d97706"
                
                # # # ✅ NIEUW: Betere weergave voor ontbrekende datum/locatie
                # # date_str = None
                # # if pd.notna(ing_date):
                    # # try:
                        # # date_str = pd.to_datetime(ing_date).strftime('%d-%m-%Y')
                    # # except:
                        # # date_str = None
                
                # # # Bouw display_info op basis van beschikbare data
                # # if date_str and loc:
                    # # display_info = f"📅 {date_str} - 📍 {loc}"
                # # elif date_str:
                    # # display_info = f"📅 {date_str}"
                # # elif loc:
                    # # display_info = f"📍 {loc}"
                # # elif dl and dl. lower() not in ["nan", "none", ""]: 
                    # # # ✅ Toon Status_Detail als geen datum/locatie beschikbaar
                    # # display_info = f"ℹ️ {detail}"
                # # else: 
                    # # display_info = "⚠️ Actie nodig"
        # elif sl in ["ingeschreven", "in wachtrij", "on hold"]:
            # ing_date = row_data. get("Ingeschreven_Datum", pd.NaT)
            # loc = str(row_data.get("Ingeschreven_Locatie", "")).strip()
            # # Clean up "nan" en "None" strings
            # if loc. lower() in ["nan", "none", ""]:
                # loc = ""
            
            # # Check of datum gepasseerd is OF geen datum
            # is_past = False
            # has_date = False
            # try:
                # if pd.notna(ing_date):
                    # has_date = True
                    # is_past = pd.to_datetime(ing_date).date() < datetime.now().date()
            # except:
                # pass

            # if is_past: 
                # # Datum is gepasseerd - wacht op resultaat/export
                # icon = "⌛"
                # bg_col = "#f1f5f9"
                # left_color = "#94a3b8"
                # display_info = f"GEPASSEERD ({pd.to_datetime(ing_date).strftime('%d-%m-%Y')}) | Wacht op export"
            
            # elif not has_date and not loc:
                # # ✅ NIEUW: Geen datum EN geen locatie = probleem! 
                # # Dit is waarschijnlijk iemand die ziek was of niet kon komen
                # icon = "⚠️"
                # bg_col = "#fef3c7"  # Geel/amber achtergrond
                # left_color = "#f59e0b"  # Amber rand
                
                # # Check of er een detail is dat we kunnen tonen
                # if detail and dl not in ["nan", "none", ""]:
                    # # Maar voeg een waarschuwing toe
                    # display_info = f"⚠️ Opnieuw inplannen | {detail}"
                # else:
                    # display_info = "⚠️ Niet gevolgd - Opnieuw inplannen"
            
            # else:
                # # Normale inschrijving met datum/locatie
                # icon = "🟠"
                # bg_col = "#fff7ed"
                # left_color = "#d97706"
                
                # # Bouw display_info op basis van beschikbare data
                # date_str = None
                # if has_date:
                    # try:
                        # date_str = pd.to_datetime(ing_date).strftime('%d-%m-%Y')
                    # except:
                        # date_str = None
                
                # if date_str and loc:
                    # display_info = f"📅 {date_str} - 📍 {loc}"
                # elif date_str:
                    # display_info = f"📅 {date_str}"
                # elif loc:
                    # display_info = f"📍 {loc}"
                # else:
                    # display_info = "⚠️ Actie nodig"
        
        # elif sl in ["afgewerkt", "recent behaald"]:
            # icon = "🟢"
            # bg_col = "#f0fdf4"
            # left_color = "#10b981"
            # beh_date = row_data.get("Behaald_Datum", pd. NaT)
            # date_str = pd.to_datetime(beh_date).strftime('%d-%m-%Y') if pd.notna(beh_date) else "Afgewerkt"
            # display_info = f"Status: {date_str}"

        # # --- 4. Speciale styling voor competenties ---
        # # ✅ AANGEPAST: Alleen paars als het GEEN nieuwe taak is
        # if "vaard" in task_type.lower() or "comp" in task_type. lower():
            # if not is_past and sl not in ["afgewerkt", "recent behaald"]:
                # # Alleen paars als het NIET een nieuwe blauwe taak is
                # if not is_new_item:
                    # left_color = "#9333ea"
                    # bg_col = "#f3e8ff"

        # # --- 5. Stylesheet en Layout ---
        # border_color = self._get_border_color(left_color)
        # self.setStyleSheet(f"""
            # TodoRowWidget {{
                # background-color: {bg_col};
                # border: 1px solid {border_color};
                # border-left:  5px solid {left_color};
                # margin: 2px 0;
            # }}
            # TodoRowWidget[selected="true"] {{
                # background-color: #e0f2fe;
                # border:  2px solid #005EB8;
            # }}
        # """)

        # main_layout = QVBoxLayout(self)
        # main_layout.setContentsMargins(10, 10, 10, 10)
        # main_layout.setSpacing(8)
        
        # top_row = QHBoxLayout()
        # task_icon = "📊" if "vaard" in task_type.lower() else "📜"
        # title_text = f"<html>{icon} {task_icon} <b>{medewerker}</b> – {cert}</html>"
        # lbl_title = QLabel(title_text)
        # lbl_title. setTextFormat(Qt.TextFormat. RichText)
        # lbl_title.setStyleSheet("font-size: 15px; color:  #1f2937; background: transparent;")
        # top_row.addWidget(lbl_title, 1)

        # lbl_status = QLabel(f"{status.upper()} | {display_info}")
        # lbl_status.setStyleSheet("font-size: 14px; color: #64748b; font-weight: 500; background: transparent;")
        # lbl_status.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        # top_row.addWidget(lbl_status)
        # main_layout.addLayout(top_row)

    # def _format_expiry(self, expiry_date):
        # if not pd. notna(expiry_date):
            # return "-"
        # try: 
            # today = datetime. now().date()
            # dt = pd. to_datetime(expiry_date).date()
            # days = (dt - today).days
            # if days < 0:
                # return f"VERLOPEN ({abs(days)} dgn geleden)"
            # return dt.strftime('%d-%m-%Y')
        # except: 
            # return "-"

    # def _get_border_color(self, left_color):
        # color = QColor(left_color)
        # return QColor(
            # min(240, color.red() + 50),
            # min(240, color.green() + 50),
            # min(240, color. blue() + 50)
        # ).name()


    # def _format_expiry(self, expiry_date):
        # if not pd.notna(expiry_date): return "-"
        # try:
            # today = datetime.now().date()
            # dt = pd.to_datetime(expiry_date).date()
            # days = (dt - today).days
            # if days < 0: return f"VERLOPEN ({abs(days)} dgn geleden)"
            # return dt.strftime('%d-%m-%Y')
        # except: return "-"

    # def _get_border_color(self, left_color):
        # color = QColor(left_color)
        # return QColor(min(240, color.red() + 50), min(240, color.green() + 50), min(240, color.blue() + 50)).name()
# ===============================================================
# BESTAND: xaurum/ui/widgets.py (GEWIJZIGDE TodoRowWidget)
# ===============================================================
class TodoRowWidget(QWidget):
    def __init__(self, row_data: pd.Series):
        super().__init__()
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.row_data = row_data

        # --- 1. Data ophalen ---
        status = str(row_data.get("Status", ""))
        detail = str(row_data.get("Status_Detail", ""))
        medewerker = str(row_data.get("MedewerkerNaam", "-"))
        task_type = str(row_data.get("TaskType", "")).strip()
        sl = status.lower()
        dl = detail.lower()
        
        # 🔧 FIX: Robuuste CertName handling
        def _safe_cert_name(val):
            if pd.isna(val): return ""
            s = str(val).strip()
            return "" if s.lower() == "nan" else s
        
        cert = _safe_cert_name(row_data.get("CertName_display", "")) or \
               _safe_cert_name(row_data.get("CertName", "")) or \
               _safe_cert_name(row_data.get("competence", "")) or "?"

        # --- 2. INITIALISATIE (FIX: is_past toegevoegd!) ---
        is_past = False  # <--- DEZE REGEL ONTBRAK
        display_info = ""
        icon = "⚪"
        bg_col = "#f0fdf4"
        left_color = "#10b981"
        is_new_item = False 
        
        # --- 3. Styling bepalen ---
        
        # A. FAILED / HERKANSING (Hoogste prio)
        if "niet geslaagd" in dl or "failed" in dl or "gezakt" in dl: 
            icon = "⛔"
            bg_col = "#ffcdd2"
            left_color = "#dc2626"
            display_info = "Herkansing nodig"
            
        # B. OPEN TAKEN (Nog in te plannen)
        elif sl == "open":
            exp_dt = row_data.get("ExpiryDate", pd.NaT)
            expiry_str = self._format_expiry(exp_dt)
            
            # Check Nieuw
            if "nieuw certificaat nodig" in dl or "vaardigheid nog niet behaald" in dl:
                icon = "🔵"
                bg_col = "#eff6ff"  # Lichtblauw
                left_color = "#3b82f6"  # Blauw
                is_new_item = True
                display_info = f"🆕 NIEUW - In te plannen"
            else:
                # Verlopen
                icon = "🔴"
                bg_col = "#fff1f2"
                left_color = "#dc2626"
                display_info = f"Vervalt: {expiry_str}"
        
        # C. INGESCHREVEN / LOPENDE
        elif sl in ["ingeschreven", "in wachtrij", "on hold"]:
            ing_date = row_data.get("Ingeschreven_Datum", pd.NaT)
            loc = str(row_data.get("Ingeschreven_Locatie", "")).strip()
            if loc.lower() in ["nan", "none", ""]: loc = ""
            
            # Datums berekenen
            has_date = False
            days_ago = 0
            
            if pd.notna(ing_date):
                try:
                    dt_obj = pd.to_datetime(ing_date).date()
                    today_obj = datetime.now().date()
                    has_date = True
                    if dt_obj < today_obj:
                        is_past = True
                        days_ago = (today_obj - dt_obj).days
                except: pass

            # --- LOGICA VOOR STATUS WEERGAVE ---

            # 1. ZIEK / AFWEZIG CHECK (Voor Brian)
            failure_terms = ["ziek", "afwezig", "absent", "geannuleerd", "cancelled", "no show", "verhinderd"]
            is_absent = any(term in dl for term in failure_terms)

            if is_absent:
                # Rood: Expliciet ziek of afwezig gemeld
                icon = "⛔"
                bg_col = "#fee2e2"
                left_color = "#dc2626"
                display_info = f"AFWEZIG ({detail}) - Opnieuw inplannen"

            elif is_past:
                # Datum is voorbij. Check 'Grace Period' (10 dagen)
                if days_ago <= 10:
                    # BLAUW: Training net geweest, wachten op admin/resultaat
                    icon = "⏳"
                    bg_col = "#e0f2fe"  # Lichtblauw
                    left_color = "#3b82f6"
                    # UI geeft duidelijk aan dat de opleiding al is afgewerkt op datum,
                    # maar dat we nog wachten op de officiële resultaten.
                    display_info = f"OPLEIDING AFGEROND ({days_ago}d geleden) | Wachten op resultaat..."
                else:
                    # ORANJE: Al lang geleden, waarom nog steeds op 'Ingeschreven'?
                    icon = "⚠️"
                    bg_col = "#fff7ed"
                    left_color = "#d97706"
                    display_info = f"GEPASSEERD ({days_ago}d geleden) - Check resultaat in Xaurum"

            elif not has_date and not loc:
                # Geen datum EN geen locatie = Vaak een probleem of annulatie zonder datum
                icon = "❓"
                bg_col = "#fffbeb"
                left_color = "#f59e0b"
                display_info = f"⚠️ Datum onbekend | {detail}"
            
            else:
                # TOEKOMST: Gewoon ingeschreven
                icon = "🟠"
                bg_col = "#fff7ed"
                left_color = "#d97706"
                d_str = pd.to_datetime(ing_date).strftime('%d-%m-%Y') if has_date else "?"
                display_info = f"📅 {d_str} - 📍 {loc or 'Locatie?'}"
        
        # D. AFGEWERKT
        elif sl in ["afgewerkt", "recent behaald"]:
            icon = "🟢"
            bg_col = "#f0fdf4"
            left_color = "#10b981"
            beh_date = row_data.get("Behaald_Datum", pd.NaT)
            date_str = pd.to_datetime(beh_date).strftime('%d-%m-%Y') if pd.notna(beh_date) else "Afgewerkt"
            display_info = f"Status: {date_str}"

        # --- 4. Speciale styling voor competenties ---
        if "vaard" in task_type.lower() or "comp" in task_type.lower():
            # Alleen paars als het geen speciale status (zoals nieuw/afwezig) heeft
            if not is_past and sl not in ["afgewerkt", "recent behaald"] and not is_new_item and icon != "⛔":
                left_color = "#9333ea"
                bg_col = "#f3e8ff"

        # --- 5. Stylesheet en Layout ---
        border_color = self._get_border_color(left_color)
        self.setStyleSheet(f"""
            TodoRowWidget {{
                background-color: {bg_col};
                border: 1px solid {border_color};
                border-left:  5px solid {left_color};
                margin: 2px 0;
            }}
            TodoRowWidget[selected="true"] {{
                background-color: #e0f2fe;
                border:  2px solid #005EB8;
            }}
        """)

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(8)
        
        top_row = QHBoxLayout()
        task_icon = "📊" if "vaard" in task_type.lower() else "📜"
        
        # Titel opbouw
        title_text = f"<html>{icon} {task_icon} <b>{medewerker}</b> – {cert}</html>"
        lbl_title = QLabel(title_text)
        lbl_title.setTextFormat(Qt.TextFormat.RichText)
        lbl_title.setStyleSheet("font-size: 15px; color: #1f2937; background: transparent;")
        top_row.addWidget(lbl_title, 1)

        # Status Label
        lbl_status = QLabel(f"{status.upper()} | {display_info}")
        lbl_status.setStyleSheet("font-size: 14px; color: #64748b; font-weight: 500; background: transparent;")
        lbl_status.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        top_row.addWidget(lbl_status)
        
        main_layout.addLayout(top_row)

    def _format_expiry(self, expiry_date):
        if not pd.notna(expiry_date): return "-"
        try:
            today = datetime.now().date()
            dt = pd.to_datetime(expiry_date).date()
            days = (dt - today).days
            if days < 0: return f"VERLOPEN ({abs(days)} dgn geleden)"
            return dt.strftime('%d-%m-%Y')
        except: return "-"

    def _get_border_color(self, left_color):
        color = QColor(left_color)
        return QColor(min(240, color.red() + 50), min(240, color.green() + 50), min(240, color.blue() + 50)).name()

# ===============================================================
# 9. GAUGE WIDGET (VOLLEDIGE VERSIE MET WIJZERS)
# ===============================================================
class GaugeWidget(QWidget):
    def __init__(self, title: str = "", value: float = 0, color: str = "#00bcd4", parent=None):
        super().__init__(parent)
        self._title = title
        self._value = max(0, min(100, value))
        self._color = QColor(color)
        
        self.setMinimumSize(240, 240)
        self.setMaximumSize(300, 300)
        self.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)

    def set_value(self, value: float):
        self._value = max(0, min(100, value))
        self.update()

    def set_color(self, color: str):
        self._color = QColor(color)
        self.update()

    def set_title(self, title: str):
        self._title = title
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        size = min(self.width(), self.height())
        margin = 15
        rect = QRectF(
            (self.width() - size) / 2 + margin,
            (self.height() - size) / 2 + margin,
            size - 2 * margin,
            size - 2 * margin
        )
        center = rect.center()
        radius = rect.width() / 2

        # 1. Background Gradient
        bg_gradient = QRadialGradient(center, radius)
        bg_gradient.setColorAt(0, QColor("#1a1a2e"))
        bg_gradient.setColorAt(0.85, QColor("#16213e"))
        bg_gradient.setColorAt(1, QColor("#0f0f1a"))
        
        painter.setBrush(QBrush(bg_gradient))
        painter.setPen(Qt.PenStyle.NoPen)
        painter.drawEllipse(rect)

        # 2. Outer Ring
        outer_pen = QPen(QColor("#2a2a4a"))
        outer_pen.setWidth(3)
        painter.setPen(outer_pen)
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawEllipse(rect)

        # 3. Inner Track (Dark Grey)
        arc_rect = rect.adjusted(18, 18, -18, -18)
        arc_pen = QPen(QColor("#2d2d4a"))
        arc_pen.setWidth(12)
        arc_pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        painter.setPen(arc_pen)
        
        start_angle = 225 * 16
        span_angle = -270 * 16
        painter.drawArc(arc_rect, start_angle, span_angle)

        # 4. Value Arc (Colored)
        if self._value > 0:
            glow_pen = QPen(self._color)
            glow_pen.setWidth(20)
            glow_pen.setCapStyle(Qt.PenCapStyle.RoundCap)
            glow_color = QColor(self._color)
            glow_color.setAlpha(60)
            glow_pen.setColor(glow_color)
            painter.setPen(glow_pen)
            value_span = int(-270 * 16 * self._value / 100)
            painter.drawArc(arc_rect, start_angle, value_span)
            
            value_pen = QPen(self._color)
            value_pen.setWidth(12)
            value_pen.setCapStyle(Qt.PenCapStyle.RoundCap)
            painter.setPen(value_pen)
            painter.drawArc(arc_rect, start_angle, value_span)

        # 5. Ticks
        painter.save()
        painter.translate(center)
        tick_pen = QPen(QColor("#4a4a6a"))
        tick_pen.setWidth(2)
        painter.setPen(tick_pen)
        for i in range(11): 
            angle = 225 - (i * 27) 
            inner_r = radius - 35
            outer_r = radius - 26
            x1 = inner_r * math.cos(math.radians(angle))
            y1 = -inner_r * math.sin(math.radians(angle))
            x2 = outer_r * math.cos(math.radians(angle))
            y2 = -outer_r * math.sin(math.radians(angle))
            painter.drawLine(QPointF(x1, y1), QPointF(x2, y2))
        painter.restore()

        # 6. Needle
        painter.save()
        painter.translate(center)
        needle_angle = 225 - (self._value * 2.7) 
        painter.rotate(-needle_angle + 90)
        
        needle_length = radius - 38
        needle_width = 5
        needle_path = QPainterPath()
        needle_path.moveTo(0, -needle_length) 
        needle_path.lineTo(-needle_width, 0)  
        needle_path.lineTo(0, 14)             
        needle_path.lineTo(needle_width, 0)   
        needle_path.closeSubpath()
        
        needle_gradient = QLinearGradient(0, -needle_length, 0, 14)
        needle_gradient.setColorAt(0, self._color)
        needle_gradient.setColorAt(0.6, self._color.darker(120))
        needle_gradient.setColorAt(1, QColor("#333344"))
        
        painter.setBrush(QBrush(needle_gradient))
        painter.setPen(QPen(self._color.darker(150), 1))
        painter.drawPath(needle_path)
        
        painter.setBrush(QBrush(QColor("#2a2a4a")))
        painter.setPen(QPen(self._color, 3))
        painter.drawEllipse(QPointF(0, 0), 12, 12)
        
        painter.setBrush(QBrush(self._color))
        painter.setPen(Qt.PenStyle.NoPen)
        painter.drawEllipse(QPointF(0, 0), 6, 6)
        painter.restore()

        # 7. Text
        font = painter.font()
        font.setPixelSize(46)
        font.setBold(True)
        painter.setFont(font)
        painter.setPen(QColor("#ffffff"))
        
        text = f"{int(self._value)}%"
        text_rect = QRectF(rect.x(), rect.y() + rect.height() * 0.28, rect.width(), rect.height() * 0.35)
        painter.drawText(text_rect, Qt.AlignmentFlag.AlignCenter, text)

        font.setPixelSize(18)
        font.setBold(False)
        painter.setFont(font)
        painter.setPen(QColor("#8888aa"))
        
        title_rect = QRectF(rect.x(), rect.y() + rect.height() * 0.60, rect.width(), rect.height() * 0.2)
        painter.drawText(title_rect, Qt.AlignmentFlag.AlignCenter, self._title)
