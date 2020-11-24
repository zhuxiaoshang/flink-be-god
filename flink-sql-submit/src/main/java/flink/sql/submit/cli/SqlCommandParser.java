package flink.sql.submit.cli;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: zhushang
 * @create: 2020-11-05 17:07
 **/

public class SqlCommandParser {
    private SqlCommandParser() {
        // private
    }

    public static List<SqlCommandCall> parse(List<String> lines) {
        List<SqlCommandCall> calls = new ArrayList<>();
        StringBuilder stmt = new StringBuilder();
        for (String line : lines) {
            if (line.trim().isEmpty() || line.startsWith("--")) {
                // skip empty line and comment line
                continue;
            }
            stmt.append("\n").append(line);
            if (line.trim().endsWith(";")) {
                Optional<SqlCommandCall> optionalCall = parse(stmt.toString());
                if (optionalCall.isPresent()) {
                    calls.add(optionalCall.get());
                } else {
                    throw new RuntimeException("Unsupported command '" + stmt.toString() + "'");
                }
                // clear string builder
                stmt.setLength(0);
            }
        }
        return calls;
    }

    public static Optional<SqlCommandCall> parse(String stmt) {
        // normalize
        stmt = stmt.trim();
        // remove ';' at the end
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }

        // parse
        for (SqlCommand cmd : SqlCommand.values()) {
            final Matcher matcher = cmd.pattern.matcher(stmt);
            if (matcher.matches()) {
                final String[] groups = new String[matcher.groupCount()];
                for (int i = 0; i < groups.length; i++) {
                    groups[i] = matcher.group(i + 1);
                }
                return cmd.operandConverter.apply(groups)
                        .map((operands) -> new SqlCommandCall(cmd, operands));
            }
        }
        return Optional.empty();
    }

    // --------------------------------------------------------------------------------------------

    private static final Function<String[], Optional<String[]>> NO_OPERANDS =
            (operands) -> Optional.of(new String[0]);

    private static final Function<String[], Optional<String[]>> SINGLE_OPERAND =
            (operands) -> Optional.of(new String[]{operands[0]});

    private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

    /**
     * Supported SQL commands.
     */
    public enum SqlCommand {
        INSERT_INTO(
                "(INSERT\\s+INTO.*)",
                SINGLE_OPERAND),

        CREATE_TABLE(
                "(CREATE\\s+TABLE.*)",
                SINGLE_OPERAND),

        SET(
                "SET(\\s+(\\S+)\\s*=(.*))?", // whitespace is only ignored on the left side of '='
                (operands) -> {
                    if (operands.length < 3) {
                        return Optional.empty();
                    } else if (operands[0] == null) {
                        return Optional.of(new String[0]);
                    }
                    return Optional.of(new String[]{operands[1], operands[2]});
                }),

        DROP("(DROP\\s+TABLE.*)",SINGLE_OPERAND),

        USE("(USE\\s+.*)",SINGLE_OPERAND);

        public final Pattern pattern;
        public final Function<String[], Optional<String[]>> operandConverter;

        SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
            this.pattern = Pattern.compile(matchingRegex, DEFAULT_PATTERN_FLAGS);
            this.operandConverter = operandConverter;
        }

        @Override
        public String toString() {
            return super.toString().replace('_', ' ');
        }

        public boolean hasOperands() {
            return operandConverter != NO_OPERANDS;
        }
    }

    /**
     * Call of SQL command with operands and command type.
     */
    public static class SqlCommandCall {
        public final SqlCommand command;
        public final String[] operands;

        public SqlCommandCall(SqlCommand command, String[] operands) {
            this.command = command;
            this.operands = operands;
        }

        public SqlCommandCall(SqlCommand command) {
            this(command, new String[0]);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SqlCommandCall that = (SqlCommandCall) o;
            return command == that.command && Arrays.equals(operands, that.operands);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(command);
            result = 31 * result + Arrays.hashCode(operands);
            return result;
        }

        @Override
        public String toString() {
            return command + "(" + Arrays.toString(operands) + ")";
        }
    }
}
