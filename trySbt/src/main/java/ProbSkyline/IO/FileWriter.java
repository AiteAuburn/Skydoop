public interface FileWriter {
	FileWriter append(CharSequence seq);

	FileWriter indent(int indent);

	void close();
}
