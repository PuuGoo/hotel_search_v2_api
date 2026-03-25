import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox
from verify_hotels import verify_hotels_file_case12_chain_vho_no_chrome, verify_hotels_file_ota_chrome
class HotelVerifierApp:
    def __init__(self, root):
        self.root = root
        self.root.title('Hotel OTA Verifier - Multi-threaded')
        self.root.geometry('700x380')
        self.input_var = tk.StringVar()
        self.output_var = tk.StringVar()
        self.name_threshold_var = tk.StringVar(value='75')
        self.address_threshold_var = tk.StringVar(value='60')
        self.num_workers_var = tk.StringVar(value='3')
        self.headless_var = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value='Sẵn sàng')
        self._build_ui()
    def _build_ui(self):
        main = tk.Frame(self.root, padx=12, pady=12)
        main.pack(fill='both', expand=True)
        tk.Label(main, text='File Excel input:').grid(row=0, column=0, sticky='w')
        tk.Entry(main, textvariable=self.input_var, width=68).grid(row=1, column=0, sticky='we', padx=(0, 8))
        tk.Button(main, text='Chọn file', command=self.choose_input).grid(row=1, column=1)
        tk.Label(main, text='File Excel output:').grid(row=2, column=0, sticky='w', pady=(10, 0))
        tk.Entry(main, textvariable=self.output_var, width=68).grid(row=3, column=0, sticky='we', padx=(0, 8))
        tk.Button(main, text='Lưu vào', command=self.choose_output).grid(row=3, column=1)
        threshold_frame = tk.Frame(main)
        threshold_frame.grid(row=4, column=0, columnspan=2, sticky='w', pady=(12, 0))
        tk.Label(threshold_frame, text='Ngưỡng tên:').grid(row=0, column=0, sticky='w')
        tk.Entry(threshold_frame, textvariable=self.name_threshold_var, width=8).grid(row=0, column=1, padx=(6, 16))
        tk.Label(threshold_frame, text='Ngưỡng địa chỉ:').grid(row=0, column=2, sticky='w')
        tk.Entry(threshold_frame, textvariable=self.address_threshold_var, width=8).grid(row=0, column=3, padx=(6, 16))
        tk.Label(threshold_frame, text='Số browser:').grid(row=0, column=4, sticky='w')
        tk.Entry(threshold_frame, textvariable=self.num_workers_var, width=8).grid(row=0, column=5, padx=(6, 16))
        tk.Checkbutton(threshold_frame, text='Chạy Chrome ẩn', variable=self.headless_var).grid(row=0, column=6, sticky='w')
        self.run_button_case12 = tk.Button(main, text='Chức năng 1: Case 1-2 + Chain/Branch + VHO + check URL (không Chrome)', command=self.run_verify_case12_no_chrome, height=2)
        self.run_button_case12.grid(row=5, column=0, columnspan=2, sticky='we', pady=(14, 0))
        self.run_button_chrome = tk.Button(main, text='Chức năng 2: Check link OTA bằng Google Chrome', command=self.run_verify_ota_chrome, height=2)
        self.run_button_chrome.grid(row=6, column=0, columnspan=2, sticky='we', pady=(8, 0))
        tk.Label(main, textvariable=self.status_var, fg='#1f5c99').grid(row=7, column=0, columnspan=2, sticky='w', pady=(10, 0))
        main.grid_columnconfigure(0, weight=1)
    def choose_input(self):
        path = filedialog.askopenfilename(title='Chọn file Excel input', filetypes=[('Excel files', '*.xlsx *.xls')])
        if path:
            self.input_var.set(path)
            if not self.output_var.get():
                base, ext = os.path.splitext(path)
                self.output_var.set(f'{base}_verified{ext or ".xlsx"}')
    def choose_output(self):
        path = filedialog.asksaveasfilename(title='Chọn nơi lưu file output', defaultextension='.xlsx', filetypes=[('Excel files', '*.xlsx')])
        if path:
            self.output_var.set(path)
    def _validate_common_inputs(self):
        input_path = self.input_var.get().strip()
        output_path = self.output_var.get().strip()
        if not input_path:
            messagebox.showerror('Thiếu dữ liệu', 'Vui lòng chọn file Excel input.')
            return None
        if not os.path.exists(input_path):
            messagebox.showerror('Sai đường dẫn', 'File input không tồn tại.')
            return None
        if not output_path:
            messagebox.showerror('Thiếu dữ liệu', 'Vui lòng chọn file output.')
            return None
        return input_path, output_path

    def run_verify_case12_no_chrome(self):
        checked = self._validate_common_inputs()
        if not checked:
            return
        input_path, output_path = checked
        self.run_button_case12.config(state='disabled')
        self.run_button_chrome.config(state='disabled')
        self.status_var.set('Đang xử lý chức năng 1...')
        worker = threading.Thread(target=self._verify_worker_case12_no_chrome, args=(input_path, output_path), daemon=True)
        worker.start()

    def run_verify_ota_chrome(self):
        checked = self._validate_common_inputs()
        if not checked:
            return
        input_path, output_path = checked
        try:
            name_threshold = int(self.name_threshold_var.get())
            address_threshold = int(self.address_threshold_var.get())
            num_workers = int(self.num_workers_var.get())
            if num_workers < 1 or num_workers > 10:
                raise ValueError('Số browser phải từ 1-10')
        except ValueError as e:
            messagebox.showerror('Sai tham số', str(e))
            return
        self.run_button_case12.config(state='disabled')
        self.run_button_chrome.config(state='disabled')
        self.status_var.set('Đang xử lý chức năng 2...')
        worker = threading.Thread(target=self._verify_worker_ota_chrome, args=(input_path, output_path, name_threshold, address_threshold, num_workers, self.headless_var.get()), daemon=True)
        worker.start()

    def _verify_worker_case12_no_chrome(self, input_path, output_path):
        def progress(done, total):
            self.root.after(0, lambda: self.status_var.set(f'Chức năng 1 đang xử lý: {done}/{total}'))
        try:
            verify_hotels_file_case12_chain_vho_no_chrome(input_path=input_path, output_path=output_path, progress_callback=progress)
            self.root.after(0, lambda: self._on_success(output_path))
        except Exception as ex:
            self.root.after(0, lambda: self._on_error(str(ex)))

    def _verify_worker_ota_chrome(self, input_path, output_path, name_threshold, address_threshold, num_workers, headless):
        def progress(done, total):
            self.root.after(0, lambda: self.status_var.set(f'Chức năng 2 đang xử lý: {done}/{total}'))
        try:
            verify_hotels_file_ota_chrome(input_path=input_path, output_path=output_path, name_threshold=name_threshold, address_threshold=address_threshold, num_workers=num_workers, headless=headless, progress_callback=progress)
            self.root.after(0, lambda: self._on_success(output_path))
        except Exception as ex:
            self.root.after(0, lambda: self._on_error(str(ex)))
    def _on_success(self, output_path):
        self.run_button_case12.config(state='normal')
        self.run_button_chrome.config(state='normal')
        self.status_var.set(f'Hoàn tất: {output_path}')
        messagebox.showinfo('Thành công', f'Đã xuất file kết quả:\n{output_path}')
    def _on_error(self, error_text):
        self.run_button_case12.config(state='normal')
        self.run_button_chrome.config(state='normal')
        self.status_var.set('Lỗi khi xử lý')
        messagebox.showerror('Lỗi', error_text)
def main():
    root = tk.Tk()
    app = HotelVerifierApp(root)
    root.mainloop()
if __name__ == '__main__':
    main()
