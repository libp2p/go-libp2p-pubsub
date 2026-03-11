const fs = require('fs');
const file = '/home/sukun/.pi/agent/extensions/todo.ts';
let content = fs.readFileSync(file, 'utf8');

const newClass = `class TodoBoardComponent {
	private selected = 0;
	private editMode = false;

	constructor(
		private readonly store: TodoRepoStore,
		private readonly theme: Theme,
		private readonly tui: { requestRender: () => void },
		private readonly done: () => void,
	) {}

	handleInput(data: string): void {
		const task = ensureDefaultTask(this.store);

		if (this.editMode) {
			if (matchesKey(data, "escape") || matchesKey(data, "enter")) {
				this.editMode = false;
				void saveStore(this.store);
				this.tui.requestRender();
				return;
			}
			if (matchesKey(data, "backspace") || data === "\\x7f") {
				const todo = task.todos[this.selected];
				if (todo && todo.text.length > 0) {
					todo.text = todo.text.slice(0, -1);
					todo.updatedAt = now();
					this.tui.requestRender();
				}
				return;
			}
			if (!data.startsWith("\\x1b") && data.length === 1 && data.charCodeAt(0) >= 32 && data.charCodeAt(0) !== 127) {
				const todo = task.todos[this.selected];
				if (todo) {
					todo.text += data;
					todo.updatedAt = now();
					this.tui.requestRender();
				}
			}
			return;
		}

		if (matchesKey(data, "escape") || matchesKey(data, "ctrl+c")) {
			this.done();
			return;
		}
		if (matchesKey(data, "up") || data === "k") {
			this.selected = Math.max(0, this.selected - 1);
			this.tui.requestRender();
			return;
		}
		if (matchesKey(data, "down") || data === "j") {
			this.selected = Math.min(Math.max(0, task.todos.length - 1), this.selected + 1);
			this.tui.requestRender();
			return;
		}
		if (matchesKey(data, "space") || matchesKey(data, "enter")) {
			const todo = task.todos[this.selected];
			if (!todo) return;
			todo.done = !todo.done;
			todo.updatedAt = now();
			historyEntry(this.store, task, "todo_toggled", "user", {
				todoId: todo.id,
				todoText: todo.text,
				state: todo.done ? "done" : "needs-work",
			});
			setTaskCompletion(task);
			void saveStore(this.store);
			this.tui.requestRender();
			return;
		}
		if (data === "c") {
			void this.clearTodos();
			return;
		}
		if (data === "i") {
			const todo = task.todos[this.selected];
			if (todo) {
				this.editMode = true;
				this.tui.requestRender();
			}
			return;
		}
		if (data === "d") {
			const todo = task.todos[this.selected];
			if (todo) {
				task.todos.splice(this.selected, 1);
				if (this.selected >= task.todos.length && this.selected > 0) {
					this.selected--;
				}
				historyEntry(this.store, task, "todo_cleared", "user", { note: \`deleted todo #\${todo.id}\` });
				setTaskCompletion(task);
				void saveStore(this.store);
				this.tui.requestRender();
			}
			return;
		}
	}

	private async clearTodos(): Promise<void> {
		const task = ensureDefaultTask(this.store);
		const countsBefore = task.todos.length;
		task.todos = [];
		task.nextTodoId = 1;
		task.completedAt = undefined;
		historyEntry(this.store, task, "todo_cleared", "user", { note: \`cleared \${countsBefore} todos\` });
		void saveStore(this.store);
		this.selected = 0;
		this.tui.requestRender();
	}

	render(width: number): string[] {
		const task = ensureDefaultTask(this.store);
		const counts = taskCounts(task);
		const lines: string[] = [];
		lines.push("");
		const modeStr = this.editMode ? this.theme.fg("warning", " [INSERT]") : "";
		lines.push(truncateToWidth(this.theme.fg("borderMuted", \` Todo: \${task.name} \`) + this.theme.fg("muted", \` \${counts.done}/\${counts.total}\`) + modeStr, width));
		lines.push("");
		if (task.todos.length === 0) {
			lines.push(truncateToWidth(\`  \${this.theme.fg("dim", "No todos yet. Use /todo-add to create one.")}\`, width));
		} else {
			for (let i = 0; i < task.todos.length; i++) {
				const todo = task.todos[i]!;
				const selected = i === this.selected;
				const prefix = selected ? this.theme.fg("accent", ">") : " ";
				const mark = todo.done ? this.theme.fg("success", "✓") : this.theme.fg("dim", "○");
				let text = todo.done ? this.theme.fg("dim", todo.text) : this.theme.fg("text", todo.text);
				if (selected && this.editMode) {
					text = todo.text + this.theme.bg("accent", " ");
				}
				lines.push(truncateToWidth(\`  \${prefix} \${mark} #\${todo.id} \${text}\`, width));
			}
		}
		lines.push("");
		if (this.editMode) {
			lines.push(truncateToWidth(\`  \${this.theme.fg("dim", "Enter/Esc save & close")}\`, width));
		} else {
			lines.push(truncateToWidth(\`  \${this.theme.fg("dim", "↑↓/k/j move")}	\${this.theme.fg("dim", "space toggle")}	\${this.theme.fg("dim", "i edit")}	\${this.theme.fg("dim", "d delete")}	\${this.theme.fg("dim", "c clear")}	\${this.theme.fg("dim", "Esc close")}\`, width));
		}
		lines.push("");
		return lines;
	}

	invalidate(): void {
		this.tui.requestRender();
	}
}`;

const oldClassRegex = /class TodoBoardComponent \{[\s\S]*?invalidate\(\): void \{\n\t\tthis\.tui\.requestRender\(\);\n\t\}\n\}/;
content = content.replace(oldClassRegex, newClass);

fs.writeFileSync(file, content);
