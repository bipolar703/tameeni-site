import sys
import json
from pathlib import Path
from typing import List, Union
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QVBoxLayout,
    QHBoxLayout,
    QWidget,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QMessageBox,
    QFormLayout,
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QIcon

class AdminPanel(QMainWindow):
    def __init__(self, output_dir: str) -> None:
        super().__init__()
        self.output_dir: Path = Path(output_dir)
        self.links_data: List[dict] = self.load_links_data()
        self.initUI()

    def load_links_data(self) -> List[dict]:
        try:
            with open(self.output_dir / 'customizable_links.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            QMessageBox.critical(self, "Error", "customizable_links.json not found. Please copy a website first.")
            sys.exit(1)
        except json.JSONDecodeError:
            QMessageBox.critical(self, "Error", "Failed to parse customizable_links.json. Check the file format.")
            sys.exit(1)

    def initUI(self) -> None:
        self.setWindowTitle('Website Customization Admin Panel')
        self.setGeometry(100, 100, 800, 600)
        self.setWindowIcon(QIcon('icon.png'))

        central_widget: QWidget = QWidget()
        self.setCentralWidget(central_widget)

        main_layout: QVBoxLayout = QVBoxLayout(central_widget)

        scroll_area: QScrollArea = QScrollArea()
        scroll_area.setWidgetResizable(True)
        main_layout.addWidget(scroll_area)

        scroll_content: QWidget = QWidget()
        scroll_layout: QVBoxLayout = QVBoxLayout(scroll_content)

        for link in self.links_data:
            link_widget: QWidget = self.create_link_widget(link)
            scroll_layout.addWidget(link_widget)

        scroll_area.setWidget(scroll_content)

        save_button: QPushButton = QPushButton('Save Changes')
        save_button.clicked.connect(self.save_changes)
        main_layout.addWidget(save_button)

    def create_link_widget(self, link: dict) -> QWidget:
        widget: QWidget = QWidget()
        layout: QFormLayout = QFormLayout(widget)

        layout.addRow(QLabel(f"ID: {link['id']}"), QLabel(f"Text: {link['text']}"))

        url_input: QLineEdit = QLineEdit(link['original_url'])
        url_input.setMinimumWidth(300)
        layout.addRow(QLabel("Original URL:"), url_input)

        link['url_input'] = url_input

        return widget

    def save_changes(self) -> None:
        try:
            for link in self.links_data:
                link['original_url'] = link['url_input'].text().strip()

            with open(self.output_dir / 'customizable_links.json', 'w', encoding='utf-8') as f:
                json.dump(self.links_data, f, indent=2)

            html_file: Path = self.output_dir / 'index.html'
            with open(html_file, 'r', encoding='utf-8') as f:
                content: str = f.read()

            for link in self.links_data:
                new_url: str = link['url_input'].text().strip()
                content = content.replace(
                    f'data-custom-link="{link["id"]}" href="{link["original_url"]}"',
                    f'data-custom-link="{link["id"]}" href="{new_url}"'
                )

            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(content)

            QMessageBox.information(self, "Success", "Changes saved successfully!")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred while saving changes: {str(e)}")

def run_admin_panel(output_dir: str) -> None:
    app: QApplication = QApplication(sys.argv)
    ex: AdminPanel = AdminPanel(output_dir)
    ex.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python admin_panel.py <output_directory>")
        sys.exit(1)

    output_dir: str = sys.argv[1]
    run_admin_panel(output_dir)