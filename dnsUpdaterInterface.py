import tkinter as tk
from tkinter import messagebox
import threading
from dnsUpdater import DNSUpdater  # Assuming your class is in dns_updater.py


class DNSUpdaterInterface:
    def __init__(self, master, dns_updater):
        self.master = master
        self.dns_updater = dns_updater
        self.running = False

        self.master.title("DNS Updater Interface")
        self.master.geometry("400x200")

        # Buttons for actions

        self.update_info_btn = tk.Button(master, text="Update Product Info", command=self.execute_all_tasks)
        self.update_info_btn.pack(pady=10)

        self.start_update_prices_btn = tk.Button(master, text="Start Update Prices", command=self.start_update_prices)
        self.start_update_prices_btn.pack(pady=10)

        self.stop_update_prices_btn = tk.Button(master, text="Stop Update Prices", command=self.stop_update_prices, state=tk.DISABLED)
        self.stop_update_prices_btn.pack(pady=10)

    def execute_all_tasks(self):
        """Запускает функции поочередно: load_products, load_names, update_product_info."""
        threading.Thread(target=self._run_all_tasks, daemon=True).start()

    def _run_all_tasks(self):
        """Последовательно выполняет все задачи."""
        try:
            self.dns_updater.load_products()
            print("load_products tasks completed successfully!")
            self.dns_updater.load_name()
            print("load_name tasks completed successfully!")
            self.dns_updater.update_product_info()
            print("update_product_info tasks completed successfully!")
            print("All tasks completed successfully!")
        except Exception as e:
            print(f"An error occurred: {e}")

    def start_update_prices(self):
        self.dns_updater.running = True  # Устанавливаем флаг в True
        self.start_update_prices_btn.config(state=tk.DISABLED)
        self.stop_update_prices_btn.config(state=tk.NORMAL)
        threading.Thread(target=self._update_prices_loop, daemon=True).start()

    def stop_update_prices(self):
        self.dns_updater.running = False  # Устанавливаем флаг в False
        self.start_update_prices_btn.config(state=tk.NORMAL)
        self.stop_update_prices_btn.config(state=tk.DISABLED)
        self.dns_updater.counter = 0

    def _update_prices_loop(self):
        while self.dns_updater.running:
            try:
                self.dns_updater.update_product_prices()
                self.dns_updater.counter = 0
            except Exception as e:
                print(f"An error occurred during price update: {e}")
                self.running = False
                break

    def _run_function(self, func, action_name):
        try:
            func()
            messagebox.showinfo("Success", f"{action_name} completed successfully.")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during {action_name}: {e}")

# Example usage
if __name__ == "__main__":
    dns_updater = DNSUpdater()
    root = tk.Tk()
    app = DNSUpdaterInterface(root, dns_updater)
    root.mainloop()
