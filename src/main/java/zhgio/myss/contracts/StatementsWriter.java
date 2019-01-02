package zhgio.myss.contracts;


public interface StatementsWriter {

	void writeCreateStatement();

	void writeAlterTableAddFkConstraintsStatement();
}
