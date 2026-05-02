import tkinter as tk
from tkinter import ttk, scrolledtext
import threading
import time
import queue
import random

class CommunicationSimulator:
    def __init__(self, root):
        self.root = root
        self.root.title("Simulasi Interaktif Model Komunikasi Sistem Terdistribusi")
        self.root.geometry("1200x800")

        # Model komunikasi
        self.models = ["Request-Response", "Publish-Subscribe", "Message Passing"]
        self.current_model = tk.StringVar(value=self.models[0])

        # Komponen untuk Request-Response
        self.client_queue = queue.Queue()
        self.server_queue = queue.Queue()

        # Komponen untuk Publish-Subscribe
        self.publisher_queue = queue.Queue()
        self.subscribers = [queue.Queue() for _ in range(3)]  # 3 subscribers

        # Komponen untuk Message Passing
        self.nodes = [queue.Queue() for _ in range(4)]  # 4 nodes
        self.node_positions = [(150, 150), (450, 150), (150, 350), (450, 350)]

        # Metrik
        self.metrics = {
            "Request-Response": {"count": 0, "latency": [], "throughput": 0},
            "Publish-Subscribe": {"count": 0, "latency": [], "throughput": 0},
            "Message Passing": {"count": 0, "latency": [], "throughput": 0}
        }
        self.start_time = time.time()

        # Threading
        self.running = True
        self.threads = []

        # GUI Elements
        self.setup_gui()

        # Start simulation threads
        self.start_simulation()

    def setup_gui(self):
        # Frame utama
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Pilihan model
        model_frame = ttk.LabelFrame(main_frame, text="Pilih Model Komunikasi")
        model_frame.pack(fill=tk.X, pady=5)
        for model in self.models:
            ttk.Radiobutton(model_frame, text=model, variable=self.current_model, value=model, command=self.switch_model).pack(side=tk.LEFT, padx=10)

        # Skenario dunia nyata
        scenario_frame = ttk.LabelFrame(main_frame, text="Skenario Dunia Nyata")
        scenario_frame.pack(fill=tk.X, pady=5)
        self.scenario_var = tk.StringVar(value="E-commerce")
        ttk.Radiobutton(scenario_frame, text="E-commerce (Client-Server)", variable=self.scenario_var, value="E-commerce", command=self.update_scenario).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(scenario_frame, text="IoT Sensors (Pub-Sub)", variable=self.scenario_var, value="IoT", command=self.update_scenario).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(scenario_frame, text="Distributed Computing (Message Passing)", variable=self.scenario_var, value="Distributed", command=self.update_scenario).pack(side=tk.LEFT, padx=10)

        # Canvas untuk visualisasi
        self.canvas = tk.Canvas(main_frame, bg="white", height=400)
        self.canvas.pack(fill=tk.BOTH, expand=True, pady=5)

        # Frame kontrol
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=5)

        # Kontrol Request-Response
        self.rr_frame = ttk.LabelFrame(control_frame, text="Request-Response (E-commerce)")
        ttk.Label(self.rr_frame, text="Request (e.g., 'Get product info'):").pack()
        self.rr_message = ttk.Entry(self.rr_frame, width=40)
        self.rr_message.pack(pady=5)
        ttk.Button(self.rr_frame, text="Kirim Request", command=self.send_request).pack(pady=5)

        # Kontrol Publish-Subscribe
        self.ps_frame = ttk.LabelFrame(control_frame, text="Publish-Subscribe (IoT)")
        ttk.Label(self.ps_frame, text="Sensor Data (e.g., 'Temperature: 25°C'):").pack()
        self.ps_message = ttk.Entry(self.ps_frame, width=40)
        self.ps_message.pack(pady=5)
        ttk.Button(self.ps_frame, text="Publish Data", command=self.publish_message).pack(pady=5)

        # Kontrol Message Passing
        self.mp_frame = ttk.LabelFrame(control_frame, text="Message Passing (Distributed)")
        ttk.Label(self.mp_frame, text="From Node:").grid(row=0, column=0)
        self.mp_from = ttk.Combobox(self.mp_frame, values=["Node 1", "Node 2", "Node 3", "Node 4"], state="readonly")
        self.mp_from.current(0)
        self.mp_from.grid(row=0, column=1)
        ttk.Label(self.mp_frame, text="To Node:").grid(row=1, column=0)
        self.mp_to = ttk.Combobox(self.mp_frame, values=["Node 1", "Node 2", "Node 3", "Node 4"], state="readonly")
        self.mp_to.current(1)
        self.mp_to.grid(row=1, column=1)
        ttk.Label(self.mp_frame, text="Message:").grid(row=2, column=0)
        self.mp_message = ttk.Entry(self.mp_frame, width=30)
        self.mp_message.grid(row=2, column=1)
        ttk.Button(self.mp_frame, text="Send Message", command=self.send_mp_message).grid(row=3, column=0, columnspan=2, pady=5)

        # Log dan metrik
        bottom_frame = ttk.Frame(main_frame)
        bottom_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.log_text = scrolledtext.ScrolledText(bottom_frame, height=10)
        self.log_text.pack(fill=tk.BOTH, expand=True)

        metrics_frame = ttk.LabelFrame(bottom_frame, text="Metrik Perbandingan")
        metrics_frame.pack(fill=tk.X, pady=5)
        self.metrics_labels = {}
        for model in self.models:
            self.metrics_labels[model] = ttk.Label(metrics_frame, text=f"{model}: Count=0, Avg Latency=0ms, Throughput=0 msg/s")
            self.metrics_labels[model].pack(side=tk.LEFT, padx=10)

        self.switch_model()
        self.update_scenario()

    def switch_model(self):
        model = self.current_model.get()
        if model == "Request-Response":
            self.rr_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5)
            self.ps_frame.pack_forget()
            self.mp_frame.pack_forget()
            self.draw_rr_diagram()
        elif model == "Publish-Subscribe":
            self.ps_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5)
            self.rr_frame.pack_forget()
            self.mp_frame.pack_forget()
            self.draw_ps_diagram()
        else:  # Message Passing
            self.mp_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5)
            self.rr_frame.pack_forget()
            self.ps_frame.pack_forget()
            self.draw_mp_diagram()

    def update_scenario(self):
        scenario = self.scenario_var.get()
        if scenario == "E-commerce":
            self.current_model.set("Request-Response")
        elif scenario == "IoT":
            self.current_model.set("Publish-Subscribe")
        else:
            self.current_model.set("Message Passing")
        self.switch_model()

    def draw_rr_diagram(self):
        self.canvas.delete("all")
        # Client
        self.canvas.create_oval(100, 150, 200, 250, fill="lightblue")
        self.canvas.create_text(150, 200, text="Client\n(E-commerce App)")
        # Server
        self.canvas.create_oval(600, 150, 700, 250, fill="lightgreen")
        self.canvas.create_text(650, 200, text="Server\n(Database)")
        # Arrow
        self.canvas.create_line(200, 200, 600, 200, arrow=tk.LAST)

    def draw_ps_diagram(self):
        self.canvas.delete("all")
        # Publisher
        self.canvas.create_oval(100, 150, 200, 250, fill="lightcoral")
        self.canvas.create_text(150, 200, text="Publisher\n(IoT Sensor)")
        # Subscribers
        for i in range(3):
            x = 400 + i * 150
            self.canvas.create_oval(x, 100 + i * 50, x + 100, 200 + i * 50, fill="lightyellow")
            self.canvas.create_text(x + 50, 150 + i * 50, text=f"Subscriber {i+1}\n(Monitor)")
            # Arrow from publisher
            self.canvas.create_line(200, 200, x, 150 + i * 50, arrow=tk.LAST)

    def draw_mp_diagram(self):
        self.canvas.delete("all")
        for i, pos in enumerate(self.node_positions):
            x, y = pos
            self.canvas.create_oval(x-50, y-50, x+50, y+50, fill="lightcyan")
            self.canvas.create_text(x, y, text=f"Node {i+1}\n(Worker)")
        # Lines between nodes
        self.canvas.create_line(200, 200, 500, 200, arrow=tk.LAST)
        self.canvas.create_line(200, 200, 200, 400, arrow=tk.LAST)
        self.canvas.create_line(500, 200, 500, 400, arrow=tk.LAST)
        self.canvas.create_line(200, 400, 500, 400, arrow=tk.LAST)

    def animate_message(self, start_x, start_y, end_x, end_y, message):
        msg_id = self.canvas.create_text(start_x, start_y, text=message, fill="red")
        dx = (end_x - start_x) / 20
        dy = (end_y - start_y) / 20
        def move():
            nonlocal msg_id
            self.canvas.move(msg_id, dx, dy)
            current_pos = self.canvas.coords(msg_id)
            if abs(current_pos[0] - end_x) > abs(dx) or abs(current_pos[1] - end_y) > abs(dy):
                self.root.after(50, move)
            else:
                self.canvas.delete(msg_id)
        move()

    def send_request(self):
        message = self.rr_message.get()
        if message:
            start_time = time.time()
            self.client_queue.put((message, start_time))
            self.log_message(f"Client mengirim request: {message}")
            self.animate_message(200, 200, 600, 200, message)
            self.rr_message.delete(0, tk.END)

    def publish_message(self):
        message = self.ps_message.get()
        if message:
            start_time = time.time()
            self.publisher_queue.put((message, start_time))
            self.log_message(f"Publisher publish data: {message}")
            for i in range(3):
                x = 450 + i * 150
                y = 150 + i * 50
                self.animate_message(200, 200, x, y, message)
            self.ps_message.delete(0, tk.END)

    def send_mp_message(self):
        from_node = int(self.mp_from.get().split()[1]) - 1
        to_node = int(self.mp_to.get().split()[1]) - 1
        message = self.mp_message.get()
        if message and from_node != to_node:
            start_time = time.time()
            self.nodes[from_node].put((message, to_node, start_time))
            self.log_message(f"Node {from_node+1} mengirim ke Node {to_node+1}: {message}")
            start_x, start_y = self.node_positions[from_node]
            end_x, end_y = self.node_positions[to_node]
            self.animate_message(start_x, start_y, end_x, end_y, message)
            self.mp_message.delete(0, tk.END)

    def log_message(self, msg):
        self.log_text.insert(tk.END, f"[{time.strftime('%H:%M:%S')}] {msg}\n")
        self.log_text.see(tk.END)

    def update_metrics(self):
        current_time = time.time()
        elapsed = current_time - self.start_time
        for model, data in self.metrics.items():
            if data["latency"]:
                avg_latency = sum(data["latency"]) / len(data["latency"]) * 1000  # ms
            else:
                avg_latency = 0
            throughput = data["count"] / elapsed if elapsed > 0 else 0
            self.metrics_labels[model].config(text=f"{model}: Count={data['count']}, Avg Latency={avg_latency:.1f}ms, Throughput={throughput:.2f} msg/s")

    def simulate_rr(self):
        while self.running:
            try:
                message, start_time = self.client_queue.get(timeout=1)
                time.sleep(random.uniform(0.2, 0.8))  # Simulasi delay
                latency = time.time() - start_time
                response = f"Response untuk: {message}"
                self.server_queue.put(response)
                self.log_message(f"Server merespons: {response}")
                self.metrics["Request-Response"]["count"] += 1
                self.metrics["Request-Response"]["latency"].append(latency)
                self.root.after(0, self.update_metrics)
            except queue.Empty:
                pass

    def simulate_ps(self):
        while self.running:
            try:
                message, start_time = self.publisher_queue.get(timeout=1)
                time.sleep(random.uniform(0.1, 0.5))  # Simulasi delay
                latency = time.time() - start_time
                for i, sub_queue in enumerate(self.subscribers):
                    sub_queue.put(message)
                    self.log_message(f"Subscriber {i+1} menerima: {message}")
                self.metrics["Publish-Subscribe"]["count"] += 1
                self.metrics["Publish-Subscribe"]["latency"].append(latency)
                self.root.after(0, self.update_metrics)
            except queue.Empty:
                pass

    def simulate_mp(self):
        while self.running:
            for i, node_queue in enumerate(self.nodes):
                try:
                    message, to_node, start_time = node_queue.get(timeout=0.1)
                    time.sleep(random.uniform(0.3, 1.0))  # Simulasi delay
                    latency = time.time() - start_time
                    self.log_message(f"Node {to_node+1} menerima dari Node {i+1}: {message}")
                    self.metrics["Message Passing"]["count"] += 1
                    self.metrics["Message Passing"]["latency"].append(latency)
                    self.root.after(0, self.update_metrics)
                except queue.Empty:
                    pass

    def start_simulation(self):
        rr_thread = threading.Thread(target=self.simulate_rr, daemon=True)
        ps_thread = threading.Thread(target=self.simulate_ps, daemon=True)
        mp_thread = threading.Thread(target=self.simulate_mp, daemon=True)
        self.threads.extend([rr_thread, ps_thread, mp_thread])
        rr_thread.start()
        ps_thread.start()
        mp_thread.start()

    def on_closing(self):
        self.running = False
        for thread in self.threads:
            thread.join(timeout=1)
        self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = CommunicationSimulator(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()