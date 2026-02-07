//#define MEASURE
//#define MEASURE_PARSE

using Microsoft.SqlServer.Management.SqlParser.Metadata;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Dynamic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

var sql = """
    select name, age
    from example
    where age = 20
    """;

#if MEASURE
var start1 = Stopwatch.GetTimestamp();
var cc = Microsoft.SqlServer.Management.SqlParser.Parser.Parser.Parse(sql);
var end1 = Stopwatch.GetElapsedTime(start1);
Console.WriteLine("1 - {0}ms", end1.TotalMilliseconds);
#endif

//Microsoft.SqlServer.Management.SqlParser.Parser.Token t = cc.Script.Tokens.First();

#if MEASURE_PARSE
var start2 = Stopwatch.GetTimestamp();
var tokee = LexerParserExec.GetTokens(sql);
var end2 = Stopwatch.GetElapsedTime(start2);
Console.WriteLine("2 - {0}ms", end2.TotalMilliseconds);
LexerParserExec.PrintTokens(sql, tokee);
#endif

var start3 = Stopwatch.GetTimestamp();
var syntaxes = LexerParserExec.Parse(sql);
var end3 = Stopwatch.GetElapsedTime(start3);
Console.WriteLine("3 - {0}ms", end3.TotalMilliseconds);

LexerParserExec.Execute(syntaxes, sql.AsMemory());

LexerParserExec.PrintSyntax(syntaxes, sql);

; ;

Console.ReadLine();

#pragma warning disable IDE0079
#pragma warning disable CA1050

public static class LexerParserExec
{
    [DoesNotReturn]
    public static void TodoErrorReport(string? report = null)
    {
        Console.WriteLine(report);
        throw new NotImplementedException();
    }

    public static (string tokenText, TokenType tokenType)[] Preserved = [
        ("FROM"  , TokenType.TOKEN_FROM            ),
        ("SELECT", TokenType.TOKEN_SELECT          ),
        ("UPDATE", TokenType.TOKEN_UPDATE          ),
        ("DELETE", TokenType.TOKEN_DELETE          ),
        ("WHERE" , TokenType.TOKEN_WHERE           ),
        ("ORDER" , TokenType.TOKEN_ORDER           ),
        ("GROUP" , TokenType.TOKEN_GROUP           ),
        ("BY"    , TokenType.TOKEN_BY              ),
        ("TOP"   , TokenType.TOKEN_TOP             ),
        ("AND"   , TokenType.TOKEN_AND             ),
        ("NOT"   , TokenType.TOKEN_NOT             ),
        ("OR"    , TokenType.TOKEN_OR              ),
        ("IN"    , TokenType.TOKEN_IN              ),
        ("INNNER", TokenType.JOIN_MODIFIER_INNER   ),
        ("LEFT"  , TokenType.JOIN_MODIFIER_LEFT    ),
        ("RIGHT" , TokenType.JOIN_MODIFIER_RIGHT   ),
        ("JOIN"  , TokenType.TOKNE_JOIN            ),
        (">"     , TokenType.TOKEN_GREATER_THAN    ),
        (">="    , TokenType.TOKEN_GREATER_OR_EQUAL),
        ("<"     , TokenType.TOKEN_LESS_THAN       ),
        ("<="    , TokenType.TOKEN_LESS_THAN_OR_EQUAL   ),
        ("="     , TokenType.TOKEN_EQUAL           ),
        ("!="    , TokenType.TOKEN_NOT_EQUAL       ),
        ("<>"    , TokenType.TOKEN_NOT_EQUAL       ),
    ];

    public static ReadOnlySpan<char> SingleCharTokens => [
        ',', '.', '*', '(', ')', '+', '-', '/', '%', '|', '&', '^', '~'
    ];

    public static ReadOnlySpan<TokenType> SingleCharTokenTypes => [
        TokenType.TOKEN_COMMA,
        TokenType.TOKEN_DOT  ,
        TokenType.TOKEN_STAR ,
        TokenType.PAREN_OPEN ,
        TokenType.PAREN_CLOSE,

        TokenType.TOKEN_PLUS,
        TokenType.TOKEN_MINUS,
        TokenType.TOKEN_DIVIDE,
        TokenType.TOKEN_MODULO,
        TokenType.TOKEN_BITWISE_OR,
        TokenType.TOKEN_BITWISE_AND,
        TokenType.TOKEN_BITWISE_XOR,
        TokenType.TOKEN_BITWISE_NOT,
    ];

    public static ReadOnlySpan<char> FramingTokens => [
        '[', '\'', '"'
    ];

    public static ReadOnlySpan<char> FramingTokenClosingCharTable => [
        ']', '\'', '"'
    ];

    public static ReadOnlySpan<TokenType> FramingTokenTypeTable => [
        TokenType.TOKEN_IDENTIFIER, TokenType.LITERAL_STRING, TokenType.TOKEN_IDENTIFIER
    ];

    public static ReadOnlySpan<TokenType> FramingTokenMissingClosingTypeTable => [
        TokenType.TOKEN_MISSING_BRACKET_END, TokenType.TOKEN_MISSING_QUOTE_END, TokenType.TOKEN_MISSING_DOUBLE_QUOTE_END
    ];


    public static void Execute(List<Syntaxio> syntaxios, ReadOnlyMemory<char> sql)
    {
        var fields = typeof(Example).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);

        List<Example> queryData = [
            new() { name = "p", age = 20, id = 1 },
            new() { name = "lol", age = 2033, id = 2 },
            new() { name = "peq", age = 1999, id = 31 },
        ];


        var columns = new List<(int start, int length)>();
        (int start, int length) sourceName = default;

        var listParameter = Expression.Parameter(typeof(IEnumerable<Example>), "par_list");

        var parameter = Expression.Parameter(typeof(Example), "initial_iteration");

        Func<IEnumerable<Example>, Func<Example, Example>, IEnumerable<Example>> selecct = Enumerable.Select;

        static Example Nothing(Example example)
        {
            return example;
        }

        Func<Example, Example> nothing = Nothing;

        var callDoNothing = Expression.Call(nothing.Method, parameter);

        var doNothing = Expression.Lambda<Func<Example, Example>>(callDoNothing, parameter);

        var selecctCall = Expression.Call(selecct.Method, listParameter, doNothing);

        var fieldAccessExpressions = new List<ColumnSource>(fields.Length);
        for(var fieldIndex = 0; fieldIndex < fields.Length; fieldIndex++)
        {
            var field = fields[fieldIndex];

            var accessExpression = Expression.MakeMemberAccess(parameter, field);

            fieldAccessExpressions.Add(
                new(accessExpression: accessExpression, name: field.Name.AsMemory())
            );
        }

        Expression realSource;
        
        {
            var pparameter = Expression.Parameter(typeof(Example));
            FieldInfo nameField = fields.First(x => x.Name == nameof(Example.name));

            var extractingNameField = Expression.Field(pparameter, nameField);
            var peqq = Expression.Constant("peq");
            var compareed = Expression.Equal(extractingNameField, peqq);

            var predicatLambda = Expression.Lambda<Func<Example, bool>>(compareed, pparameter);

            Func<IEnumerable<Example>, Func<Example, bool>, IEnumerable<Example>> predicat = Enumerable.Where;
            
            var sourceExpression =
                Expression.Call(predicat.Method, listParameter!, predicatLambda);

            var whereAlreadyCalled = Expression.Lambda<Func<IEnumerable<Example>, IEnumerable<Example>>>(
                body: sourceExpression,
                listParameter
            );

            var yay = whereAlreadyCalled.Compile()(queryData).ToList();

            var compareFunc  = predicatLambda.Compile();
            var rrr = queryData.Where(compareFunc).ToList();

            var makeArrayExpression = Expression.NewArrayInit(
                typeof(object),
                fieldAccessExpressions.Select(
                    x => Expression.Convert(x.AccessExpression, typeof(object))
                )
            );

            for(var columnIndex = 0; columnIndex < fieldAccessExpressions.Count; columnIndex++)
            {
                var column = fieldAccessExpressions[columnIndex];

                var accessArrayExpression = Expression.ArrayIndex(
                    array: makeArrayExpression,
                    index: Expression.Constant(columnIndex)
                );

                // note: unboxing expression to save expression's type information
                column.AccessExpression = Expression.Convert(accessArrayExpression, column.AccessExpression.Type);
                fieldAccessExpressions[columnIndex] = column;
            }

            var selectArrayLambda = Expression.Lambda<Func<Example, object[]>>(
                makeArrayExpression,
                parameter
            );

            Func<IEnumerable<Example>, Func<Example, object[]>, IEnumerable<object[]>> selector = Enumerable.Select;

            var selectArray = Expression.Call(selector.Method, listParameter, selectArrayLambda);
            realSource = selectArray;


            var selectedArray = Expression.Lambda<Func<IEnumerable<Example>, IEnumerable<object[]>>>(
                selectArray,
                listParameter
            );

            var sselecetedArrayResult = selectedArray.Compile()(queryData);
        }

        var sources = new List<SourceStruct>()
        {
            ToSelectOfArray<object[]>(
                sourceExpression   : realSource,
                columns            : fieldAccessExpressions,
                predicatExpression : null,
                newSourceName      : "example".AsMemory()
            )
        };


        var index = 0;
        for(; index < syntaxios.Count;)
        {
            var resultSelect = ParseSelect(syntaxios, ref index, sql, "_#".AsMemory(), sources);

            Func<IEnumerable<Example>, IEnumerable<object[]>> query = Expression.Lambda<Func<IEnumerable<Example>, IEnumerable<object[]>>>(
                resultSelect.SourceExpression,
                listParameter
            ).Compile();

            var queryResult = query(queryData).ToList();

            if (queryResult.Count > 0)
            {
                Console.WriteLine("Y");
            }
            else
            {
                Console.WriteLine("F");
            }
        }
    }

    public struct SourceStruct(ReadOnlyMemory<char> name, Expression sourceExpression, List<ColumnSource> columns)
    {
        public ReadOnlyMemory<char> Name             = name;
        public Expression           SourceExpression = sourceExpression;
        public List<ColumnSource>   Columns          = columns;
    }

    public struct ColumnSource(Expression accessExpression, ReadOnlyMemory<char> name)
    {
        public Expression           AccessExpression = accessExpression;
        public ReadOnlyMemory<char> Name             = name;
    }

    public static SourceStruct ParseSelect(
        List<Syntaxio> syntaxios,
        ref int index,
        ReadOnlyMemory<char> stringStorage,
        ReadOnlyMemory<char> newSourceName,
        List<SourceStruct> globalSources
    )
    {
        var selectSyntax = syntaxios[index++];

        if (selectSyntax.SyntaxCode != SyntaxCode.SYNTAX_SELECT)
        {
            // todo: error report
            TodoErrorReport("Expected to start with select");
        }

        var fromTable = default(SourceStruct);

        var predicatExpression = default(Expression);

        var nextSyntaxIndex = index + 1;
        while(nextSyntaxIndex < selectSyntax.Storage.ChildCount)
        {
            var nextSyntax = syntaxios[nextSyntaxIndex];
            if (nextSyntax.SyntaxCode
                is SyntaxCode.SYNTAX_RENAMED_IDENTIFIER
                or SyntaxCode.SYNTAX_EXPRESSION_ACCESS)
            {
                nextSyntaxIndex += nextSyntax.Storage.ChildCount;
            }
            else if(nextSyntax.SyntaxCode == SyntaxCode.SYNTAX_IDENTIFIER)
            {
                nextSyntaxIndex += 1;
            }
            else
            {
                break;
            }
        }

        if (nextSyntaxIndex < syntaxios.Count)
        {
            var fromSyntaxIndex = nextSyntaxIndex;
            var fromSyntax = syntaxios[nextSyntaxIndex];

            if (fromSyntax.SyntaxCode == SyntaxCode.SYNTAX_FROM)
            {
                var fromExpressionSyntax = syntaxios[nextSyntaxIndex + 1];

                var isNamed = false;
                var newName = default(ReadOnlyMemory<char>);

                if (fromExpressionSyntax.SyntaxCode == SyntaxCode.SYNTAX_RENAMED_IDENTIFIER)
                {
                    nextSyntaxIndex++;

                    isNamed = true;
                    newName = stringStorage.Slice(
                        fromExpressionSyntax.Storage.StringStart,
                        fromExpressionSyntax.Storage.StringLength
                    );

                    fromExpressionSyntax = syntaxios[nextSyntaxIndex + 1];
                }

                if (fromExpressionSyntax.SyntaxCode == SyntaxCode.SYNTAX_IDENTIFIER)
                {
                    var sourceIdentifier = stringStorage.Span.Slice(
                        fromExpressionSyntax.Storage.StringStart,
                        fromExpressionSyntax.Storage.StringLength
                    );

                    var found = false;

                    for (var sourceIndex = 0; sourceIndex < globalSources.Count; sourceIndex++)
                    {
                        var source = globalSources[sourceIndex];
                        if (sourceIdentifier.Equals(source.Name.Span, StringComparison.OrdinalIgnoreCase))
                        {
                            fromTable = source;
                            if (isNamed)
                            {
                                fromTable.Name = newName;
                            }

                            found = true;
                            break;
                        }
                    }

                    if(!found)
                    {
                        // todo: error report
                        TodoErrorReport($"Table not found: {sourceIdentifier}");
                    }
                }
                // todo: remove parens entirely from syntax
                else if (fromExpressionSyntax.SyntaxCode
                    is SyntaxCode.SYNTAX_PAREN_OPEN
                    or SyntaxCode.SYNTAX_PARENTHESIZED_EXPRESSION)
                {
                    nextSyntaxIndex++;
                    var subQuerySyntax = syntaxios[nextSyntaxIndex];

                    if (subQuerySyntax.SyntaxCode != SyntaxCode.SYNTAX_SELECT)
                    {
                        // todo: error report
                        TodoErrorReport("Expected SELECT");
                    }

                    if (!isNamed)
                    {
                        // todo: error report
                        TodoErrorReport("Subquery should be named");
                    }

                    // parse select
                    Debug.Assert(newName.Length > 0, "Sub-query should have a identifier");

                    fromTable = ParseSelect(syntaxios, ref index, stringStorage, newName, globalSources);
                }

                nextSyntaxIndex = fromSyntaxIndex + fromSyntax.Storage.ChildCount + 1;
            }

            if(nextSyntaxIndex < syntaxios.Count)
            {
                fromSyntax = syntaxios[nextSyntaxIndex++];
                if (fromSyntax.SyntaxCode == SyntaxCode.SYNTAX_WHERE)
                {
                    predicatExpression = ParseToExpression(syntaxios, ref nextSyntaxIndex, stringStorage, globalSources);
                }
                else
                {
                    // todo: error report;
                    TodoErrorReport("FROM or WHERE expected");
                }
            }
        }

        // process select
        var newColumnSource = new List<ColumnSource>();
        
        for (
            var syntax = syntaxios[index];
            syntax.SyntaxCode is SyntaxCode.SYNTAX_IDENTIFIER or SyntaxCode.SYNTAX_RENAMED_IDENTIFIER;
            syntax = syntaxios[index]
        )
        {
            var (expression, name) = ParseIdentifierOrAccessExpression2(syntaxios, ref index, stringStorage, globalSources);
            var nameSpan = name.Span;

            var exists = false;
            for(var newColumnSourceIndex = 0; newColumnSourceIndex < newColumnSource.Count; newColumnSourceIndex++)
            {
                if (nameSpan.Equals(newColumnSource[newColumnSourceIndex].Name.Span, StringComparison.OrdinalIgnoreCase))
                {
                    exists = true;
                    break;
                }
            }

            if(exists)
            {
                // todo: error report
                TodoErrorReport($"Column name collision: {nameSpan}");
            }

            newColumnSource.Add(
                new ColumnSource(accessExpression: expression, name)
            );
        }

        var result = ToSelectOfArray<object[]>(
            sourceExpression   : fromTable.SourceExpression!,
            columns            : newColumnSource,
            predicatExpression : predicatExpression,
            newSourceName      : newSourceName
        );

        return result;
    }

    public static SourceStruct ToSelectOfArray<TSource>(
        Expression           sourceExpression  ,
        List<ColumnSource>   columns           ,
        ReadOnlyMemory<char> newSourceName     ,
        Expression?          predicatExpression
    )
    {
        var parameterExpression = Expression.Parameter(typeof(IEnumerable<TSource>), "par_mul");
        var parameterSingle     = Expression.Parameter(typeof(TSource)             , "par_single");
        var pparameterSingle    = Expression.Parameter(typeof(TSource)             , "ppar_single");

        var makeArrayExpression = Expression.NewArrayInit(
            typeof(object),
            columns.Select(
                // TODO: this is a problem
                (x, i) => Expression.ArrayAccess(
                    parameterSingle,
                    Expression.Constant(i)
                )
            )
        );

        for(var columnIndex = 0; columnIndex < columns.Count; columnIndex++)
        {
            var column = columns[columnIndex];

            var accessArrayExpression = Expression.ArrayIndex(
                array: makeArrayExpression,
                index: Expression.Constant(columnIndex)
            );

            // note: unboxing expression to save expression's type information
            column.AccessExpression = Expression.Convert(accessArrayExpression, column.AccessExpression.Type);
            columns[columnIndex] = column;
        }

        Func<IEnumerable<TSource>, Func<TSource, object[]>, IEnumerable<object[]>> selector = Enumerable.Select;
        Func<IEnumerable<TSource>, Func<TSource, bool>    , IEnumerable<TSource>>  predicat = Enumerable.Where;

        if(predicatExpression != null)
        {
            var ppparameterSingle = Expression.Parameter(typeof(TSource), "pppar_single");
            var predicatExpr = Expression.Lambda<Func<TSource, bool>>(
                predicatExpression,
                ppparameterSingle
            );

            var predicatCall = Expression.Call(predicat.Method, sourceExpression!, predicatExpr);
            sourceExpression = predicatCall;
            /*
            sourceExpression = Expression.Lambda<Func<IEnumerable<TSource>, IEnumerable<TSource>>>(
                predicatCall,
                parameterExpression
            );
            */
        }

        var makeArrayLambda = Expression.Lambda<Func<TSource, object[]>>(
            makeArrayExpression,
            parameterSingle
        );

        var selectArrayCall = Expression.Call(selector.Method, sourceExpression!, makeArrayLambda);
        sourceExpression = selectArrayCall;
        /*
        sourceExpression    = Expression.Lambda<Func<IEnumerable<TSource>, IEnumerable<object[]>>>(
            selectArrayCall,
            parameterExpression
        );
        */

        var result = new SourceStruct(
            name    : newSourceName,
            columns : columns,
            sourceExpression: sourceExpression
        );

        return result;
    }

    public static (Expression expession, ReadOnlyMemory<char> span) ParseIdentifierOrAccessExpression2(
        List<Syntaxio> syntaxios,
        ref int syntaxIndex,
        ReadOnlyMemory<char> stringStorage,
        List<SourceStruct> sources
    )
    {
        var syntax = syntaxios[syntaxIndex];

        var isNamed = false;
        var newName = default(ReadOnlyMemory<char>);

        if (syntax.SyntaxCode == SyntaxCode.SYNTAX_RENAMED_IDENTIFIER)
        {
            syntaxIndex++;

            isNamed = true;
            newName = stringStorage.Slice(
                syntax.Storage.StringStart,
                syntax.Storage.StringLength
            );

            syntax = syntaxios[syntaxIndex + 1];
        }

        if(IsExpression(syntax.SyntaxCode)
            || syntax.SyntaxCode == SyntaxCode.SYNTAX_IDENTIFIER)
        {
            if (!isNamed)
            {
                newName = stringStorage.Slice(
                    syntax.Storage.StringStart,
                    syntax.Storage.StringLength
                );
            }

            var columnExpression = ParseToExpression(syntaxios, ref syntaxIndex, stringStorage, sources);

            return (columnExpression, newName);
        }

        if (syntax.SyntaxCode == SyntaxCode.SYNTAX_EXPRESSION_ACCESS)
        {
            var left = syntaxios[++syntaxIndex];

            if (left.SyntaxCode == SyntaxCode.SYNTAX_EXPRESSION_ACCESS)
            {
                TodoErrorReport("Max dept of accessing is 1");
            }

            var tableIdentifier = stringStorage.Span.Slice(
                left.Storage.StringStart,
                left.Storage.StringLength
            );

            var availableColumns = default(List<ColumnSource>);

            for (var sourceIndex = 0; sourceIndex < sources.Count; sourceIndex++)
            {
                var source = sources[sourceIndex];
                if (tableIdentifier.Equals(source.Name.Span, StringComparison.OrdinalIgnoreCase))
                {
                    availableColumns = source.Columns;
                    break;
                }
            }

            if (availableColumns == null)
            {
                TodoErrorReport($"Unknown table: {tableIdentifier}");
            }

            var right = syntaxios[++syntaxIndex];
            var columnIdentifier = stringStorage.Span.Slice(
                right.Storage.StringStart,
                right.Storage.StringLength
            );

            for (var columnIndex = 0; columnIndex < availableColumns.Count; columnIndex++)
            {
                var column = availableColumns[columnIndex];
                if (columnIdentifier.Equals(column.Name.Span, StringComparison.OrdinalIgnoreCase))
                {
                    return (column.AccessExpression, column.Name);
                }
            }

            TodoErrorReport($"{tableIdentifier} doesn't have a member: {columnIdentifier}");
        }

        TodoErrorReport();

        // never happens
        return default;
    }

    public static bool IsExpression(SyntaxCode syntaxCode)
    {
        return syntaxCode
            is SyntaxCode.SYNTAX_LOGIC_OR
            or SyntaxCode.SYNTAX_LOGIC_AND
            or SyntaxCode.SYNTAX_LOGIC_EQUAL
            or SyntaxCode.SYNTAX_LOGIC_NOT_EQUAL
            or SyntaxCode.SYNTAX_LOGIC_GREATER_THAN
            or SyntaxCode.SYNTAX_LOGIC_GREATER_THAN_OR_EQUAL
            or SyntaxCode.SYNTAX_LOGIC_LESS_THAN
            or SyntaxCode.SYNTAX_LOGIC_LESS_THAN_OR_EQUAL
            or SyntaxCode.SYNTAX_LOGIC_IN

            or SyntaxCode.SYNTAX_ADD
            or SyntaxCode.SYNTAX_SUBSTRACT
            or SyntaxCode.SYNTAX_MULTIPLY
            or SyntaxCode.SYNTAX_DIVIDE
            or SyntaxCode.SYNTAX_MODULO;
    }

    public static Expression ParseToExpression(
        List<Syntaxio> syntaxios,
        ref int syntaxIndex,
        ReadOnlyMemory<char> stringStorage,
        List<SourceStruct> sources)
    {
        var syntax = syntaxios[syntaxIndex];
        syntaxIndex++;

        if (IsExpression(syntax.SyntaxCode))
        {
            var leftExpression  = ParseToExpression(syntaxios, ref syntaxIndex, stringStorage, sources);
            var rightExpression = ParseToExpression(syntaxios, ref syntaxIndex, stringStorage, sources);

            var expression = syntax.SyntaxCode switch
            {
                SyntaxCode.SYNTAX_LOGIC_OR                    => Expression.Or                (leftExpression, rightExpression),
                SyntaxCode.SYNTAX_LOGIC_AND                   => Expression.And               (leftExpression, rightExpression),
                SyntaxCode.SYNTAX_LOGIC_EQUAL                 => ExpressionEquals             (leftExpression, rightExpression),
                SyntaxCode.SYNTAX_LOGIC_LESS_THAN             => Expression.LessThan          (leftExpression, rightExpression),
                SyntaxCode.SYNTAX_LOGIC_GREATER_THAN          => Expression.GreaterThan       (leftExpression, rightExpression),
                SyntaxCode.SYNTAX_LOGIC_LESS_THAN_OR_EQUAL    => Expression.LessThanOrEqual   (leftExpression, rightExpression),
                SyntaxCode.SYNTAX_LOGIC_GREATER_THAN_OR_EQUAL => Expression.GreaterThanOrEqual(leftExpression, rightExpression),
                SyntaxCode.SYNTAX_LOGIC_IN                    => InExpression                 (leftExpression, rightExpression),

                SyntaxCode.SYNTAX_ADD                         => Expression.Add               (leftExpression, rightExpression),
                SyntaxCode.SYNTAX_SUBSTRACT                   => Expression.Subtract          (leftExpression, rightExpression),
                SyntaxCode.SYNTAX_MULTIPLY                    => Expression.Multiply          (leftExpression, rightExpression),
                SyntaxCode.SYNTAX_DIVIDE                      => Expression.Divide            (leftExpression, rightExpression),_ or
                SyntaxCode.SYNTAX_MODULO                      => Expression.Modulo            (leftExpression, rightExpression),
            };

            return expression;
        }
        else if(syntax.SyntaxCode == SyntaxCode.SYNTAX_IDENTIFIER)
        {
            var columnIdentifier = stringStorage.Span.Slice(
                syntax.Storage.StringStart,
                syntax.Storage.StringLength
            );

            var foundTimes = 0;
            var sourceName = default(ReadOnlyMemory<char>);
            var columnAccessExpression = default(Expression);
            var ambiguousColumnSources = default(StringBuilder);

            for(var sourceIndex = 0; sourceIndex < sources.Count; sourceIndex++)
            {
                var source = sources[sourceIndex];
                var (tableName, sourceType, columns) = (source.Name, source.SourceExpression, source.Columns);
                for(var columnIndex = 0; columnIndex < columns.Count; columnIndex ++)
                {
                    var column = columns[columnIndex];
                    var (accessExression, columnName) = (column.AccessExpression, column.Name);
                    if(!columnIdentifier.Equals(columnName.Span, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    columnAccessExpression = accessExression;
                    foundTimes ++;

                    if(foundTimes > 1)
                    {
                        ambiguousColumnSources ??= new();

                        if(foundTimes > 2)
                        {
                            ambiguousColumnSources.Append(sourceName).Append(", ");
                        }

                        ambiguousColumnSources.Append(tableName);
                    }

                    sourceName = tableName;
                }
            }

            if(foundTimes > 1)
            {
                TodoErrorReport($"Ambiguous column name '{columnIdentifier}': found in {ambiguousColumnSources}");
            }

            if(columnAccessExpression == null)
            {
                TodoErrorReport($"Unable to find column: {columnIdentifier}");
            }

            return columnAccessExpression;
        }
        else if(syntax.SyntaxCode == SyntaxCode.SYNTAX_EXPRESSION_ACCESS)
        {
            var left = syntaxios[++syntaxIndex];

            if (left.SyntaxCode == SyntaxCode.SYNTAX_EXPRESSION_ACCESS)
            {
                // todo: error report
                TodoErrorReport("Max dept of accessing is 1");
            }

            var tableIdentifier = stringStorage.Span.Slice(
                left.Storage.StringStart,
                left.Storage.StringLength
            );

            var availableColumns = default(List<ColumnSource>);

            for (var sourceIndex = 0; sourceIndex < sources.Count; sourceIndex++)
            {
                var source = sources[sourceIndex];
                var (name, columns) = (source.Name, source.Columns);
                if (tableIdentifier.Equals(name.Span, StringComparison.OrdinalIgnoreCase))
                {
                    availableColumns = columns;
                    break;
                }
            }

            if (availableColumns == null)
            {
                TodoErrorReport($"Unknown table: {tableIdentifier}");
            }

            var right = syntaxios[++syntaxIndex];
            var columnIdentifier = stringStorage.Span.Slice(
                right.Storage.StringStart,
                right.Storage.StringLength
            );

            for (var columnIndex = 0; columnIndex < availableColumns.Count; columnIndex++)
            {
                var column = availableColumns[columnIndex];
                var (access, name) = (column.AccessExpression, column.Name);
                if (columnIdentifier.Equals(name.Span, StringComparison.OrdinalIgnoreCase))
                {
                    return access;
                }
            }

            TodoErrorReport($"{tableIdentifier} doesn't have a member: {columnIdentifier}");
        }
        else if(syntax.SyntaxCode is SyntaxCode.SYNTAX_PAREN_OPEN or SyntaxCode.SYNTAX_PAREN_CLOSE)
        {
            return ParseToExpression(syntaxios, ref syntaxIndex, stringStorage, sources);
        }
        else if (syntax.SyntaxCode == SyntaxCode.SYNTAX_CONST_INT)
        {
            return Expression.Constant(syntax.Storage.Number, typeof(int));
        }
        else if (syntax.SyntaxCode == SyntaxCode.SYNTAX_IDENTIFIER)
        {
            var (resolvedIdentifier, _) = ParseIdentifierOrAccessExpression2(syntaxios, ref syntaxIndex, stringStorage, sources);
            return resolvedIdentifier;
        }
        else if(syntax.SyntaxCode == SyntaxCode.SYNTAX_CONST_STRING)
        {
            var slice = stringStorage.Slice(syntax.Storage.StringStart + 1, syntax.Storage.StringLength - 2);
            return Expression.Constant(slice, typeof(ReadOnlyMemory<char>));
        }
        else if(syntax.SyntaxCode
            is SyntaxCode.SYNTAX_CONST_STRING_ENUMERATION
            or SyntaxCode.SYNTAX_CONST_INT_ENUMERATION)
        {
            var count = syntax.Storage.ChildCount;
            var array = new object[count];

            for(var literalIndex = 0; literalIndex < count; literalIndex++)
            {
                var literal = syntaxios[syntaxIndex++];
                array[literalIndex] = syntax.SyntaxCode switch
                {
                    SyntaxCode.SYNTAX_CONST_STRING_ENUMERATION
                        => (object)stringStorage.Slice(literal.Storage.StringStart, literal.Storage.StringLength),

                    SyntaxCode.SYNTAX_CONST_INT_ENUMERATION or _
                        => (object)literal.Storage.Number
                };
            }

            return Expression.Constant(array, typeof(object[]));
        }
        
        return null!;
    }

    public static Expression InExpression(Expression thisOne, Expression inThose)
    {
        var comparer = EqualityComparer<object>.Create(ObjectsEquals!);
        Func<IEnumerable<object>, object, IEqualityComparer<object>, bool> contains = Contains;

        var comparerArgument = Expression.Constant(comparer, typeof(EqualityComparer<object>));
        var boxedThisOne     = Expression.Convert(thisOne, typeof(object));

        //return Expression.Call(instance: null, contains.Method, inThose, thisOne, comparerArgument);
        return Expression.Call(instance: null, contains.Method, inThose, boxedThisOne, comparerArgument);
    }

    public static bool Contains(IEnumerable<object> items, object item, IEqualityComparer<object> equalityComparer)
    {
        return Enumerable.Contains(items, item, equalityComparer);
    }

    public static BinaryExpression ExpressionEquals(Expression left, Expression right)
    {
        var (leftType, rightType) = (left.Type, right.Type);

        if(leftType == rightType)
        {
            if(leftType == typeof(int))
            {
                return Expression.Equal(left, right);
            }
            else if(leftType == typeof(ReadOnlyMemory<char>))
            {
                var equalsMethod = MemoryEquals;
                return Expression.Equal(left, right, false, method: equalsMethod.Method);
            }
            else if(leftType == typeof(string))
            {
                var equalsMethod = StringsEquals;
                return Expression.Equal(left, right, false, method: equalsMethod.Method);
            }
        }

        return Expression.Equal(
            Expression.Convert(
                left,
                typeof(object)
            ),
            Expression.Convert(
                right,
                typeof(object)
            ),
            false,
            ((Func<object, object, bool>)ObjectsEquals).Method
        );
        //return Expression.Equal(left, right);
    }

    public static bool StringsEquals(string left, string right)
    {
        return SpanEquals(left, right);
    }

    public static bool MemoryToStringEquals(ReadOnlyMemory<char> left, string right)
    {
        return SpanEquals(left.Span, right);
    }

    public static bool MemoryEquals(ReadOnlyMemory<char> left, ReadOnlyMemory<char> right)
    {
        return SpanEquals(left.Span, right.Span);
    }

    public static bool SpanEquals(ReadOnlySpan<char> left, ReadOnlySpan<char> right)
    {
        return left.Equals(right, StringComparison.OrdinalIgnoreCase);
    }

    public static bool ObjectsEquals(object left, object right)
    {
        return (left, right) switch
        {
            (int leftInt, int rightInt)                        => leftInt == rightInt,
            (string leftString, string rightString)            => string.Equals(leftString, rightString, StringComparison.OrdinalIgnoreCase),
            (ReadOnlyMemory<char> leftMem, string rightString) => leftMem.Span.Equals(rightString, StringComparison.OrdinalIgnoreCase),
            (string leftString, ReadOnlyMemory<char> rightMem) => rightMem.Span.Equals(leftString, StringComparison.OrdinalIgnoreCase),
            (ReadOnlyMemory<char> leftMem, ReadOnlyMemory<char> rightMem) => leftMem.Span.Equals(rightMem.Span, StringComparison.OrdinalIgnoreCase),
            _ => object.Equals(left, right)
        };
    }

    public static void PrintTokens(ReadOnlySpan<char> source, List<Token> tokens)
    {
        var sb = new StringBuilder(128);

        var padding = 0;
        for(var i = 0; i < tokens.Count; i++)
        {
            padding = Math.Max(tokens[i].Length, padding);
        }

        for(var i = 0; i <  tokens.Count; i++)
        {
            var content = source.Slice(tokens[i].Start, tokens[i].Length);
            var tokenName = tokens[i].TokenType switch
            {
                TokenType.ERROR                  => "error",
                TokenType.TOKEN_SELECT           => "select",
                TokenType.TOKEN_STAR             => "star",
                TokenType.TOKEN_COMMA            => "comma",
                TokenType.TOKEN_DOT              => "dot",
                TokenType.TOKEN_TOP              => "top",
                TokenType.TOKEN_UPDATE           => "update",
                TokenType.TOKEN_DELETE           => "delete",
                TokenType.TOKEN_FROM             => "from",
                TokenType.LITERAL_INT            => "int",
                TokenType.LITERAL_STRING         => "string",
                TokenType.TOKEN_GREATER_THAN     => "greater than",
                TokenType.TOKEN_GREATER_OR_EQUAL => "greater than or equal to",
                TokenType.TOKEN_IN               => "in",
                TokenType.TOKEN_IDENTIFIER       => "identifier",
                TokenType.TOKEN_AND              => "&&",
                TokenType.TOKEN_OR               => "||",
                TokenType.PAREN_OPEN             => "open paren",
                TokenType.PAREN_CLOSE            => "close paren",
                _ => "unknown"
            };

            sb.Clear();

            sb.Append(content);
            sb.Append(' ', padding - tokens[i].Length);
            sb.Append("  -  ");
            sb.AppendFormat("({0})", tokenName);

            Console.WriteLine(sb);
        }
    }

    public static void PrintSyntax(List<Syntaxio> syntaxios, ReadOnlySpan<char> sql)
    {
        for(var i = 0; i < syntaxios.Count; i++)
        {
            var print = "unknown";
            var syntax = syntaxios[i];

            switch(syntax.SyntaxCode)
            {
                case SyntaxCode.SYNTAX_SELECT:
                    print = "select";
                    print = $"{print}, child count: {syntax.Storage.ChildCount}";
                    break;
                case SyntaxCode.SYNTAX_UPDATE:
                    print = "update";
                    print = $"{print}, child count: {syntax.Storage.ChildCount}";
                    break;
                case SyntaxCode.SYNTAX_DELETE:
                    print = "delete";
                    break;
                case SyntaxCode.SYNTAX_FROM:
                    print = "from";
                    print = $"{print}, child count: {syntax.Storage.ChildCount}";
                    break;
                case SyntaxCode.SYNTAX_WHERE:
                    print = "where";
                    print = $"{print}, child count: {syntax.Storage.ChildCount}";
                    break;

                case SyntaxCode.SYNTAX_ERROR:
                    print = "err";
                    break;

                case SyntaxCode.SYNTAX_ORDER_BY_ASCENDING:
                    print = "asc";
                    break;
                case SyntaxCode.SYNTAX_ORDER_BY_DESCENDING:
                    print = "desc";
                    break;

                case SyntaxCode.SYNTAX_COLUMNS_ALL:
                    print = "*";
                    break;
                case SyntaxCode.SYNTAX_IDENTIFIER:
                    var identifier = sql.Slice(syntax.Storage.StringStart, syntax.Storage.StringLength);

                    print = "identifier";
                    print = $"{print}, length: {syntax.Storage.StringLength}, value: {identifier}";
                    break;
                case SyntaxCode.SYNTAX_RENAMED_IDENTIFIER:
                    var reidentifier = sql.Slice(syntax.Storage.StringStart, syntax.Storage.StringLength);
                    
                    print = $"as {reidentifier}, child count: {syntax.Storage.ChildCount}";
                    break;

                case SyntaxCode.SYNTAX_PARENTHESIZED_EXPRESSION:
                    print = "(expr)";
                    print = $"{print}, child count: {syntax.Storage.ChildCount}";
                    break;
                case SyntaxCode.SYNTAX_PAREN_OPEN:
                    print = "(";
                    break;
                case SyntaxCode.SYNTAX_PAREN_CLOSE:
                    print = ")";
                    break;

                case SyntaxCode.SYNTAX_ADD:
                    print = "+";
                    break;
                case SyntaxCode.SYNTAX_SUBSTRACT:
                    print = "-";
                    break;
                case SyntaxCode.SYNTAX_MULTIPLY:
                    print = "x * y";
                    break;
                case SyntaxCode.SYNTAX_DIVIDE:
                    print = "x / y";
                    break;
                case SyntaxCode.SYNTAX_MODULO:
                    print = "x % y";
                    break;

                case SyntaxCode.SYNTAX_CONST_INT:
                    print = $"literal: {syntax.Storage.Number}";
                    break;
                case SyntaxCode.SYNTAX_CONST_STRING:
                    var stringLiteral = sql.Slice(syntax.Storage.StringStart, syntax.Storage.StringLength);
                    print = $"literal: \"{stringLiteral}\"";
                    break;

                case SyntaxCode.SYNTAX_EXPRESSION_ACCESS:
                    print = "x.y";
                    break;

                case SyntaxCode.SYNTAX_LOGIC_EQUAL:
                    print = "==";
                    var rightChildCount = syntax.Storage.ChildCount - syntax.Storage.LeftChildCount;
                    print = $"{print}, left child count: {syntax.Storage.LeftChildCount}, right child count: {rightChildCount}";
                    break;
                case SyntaxCode.SYNTAX_LOGIC_NOT_EQUAL:
                    print = "!=";
                    break;
                case SyntaxCode.SYNTAX_LOGIC_GREATER_THAN:
                    print = "x > y";
                    break;
                case SyntaxCode.SYNTAX_LOGIC_GREATER_THAN_OR_EQUAL:
                    print = "x >= y";
                    break;
                case SyntaxCode.SYNTAX_LOGIC_LESS_THAN:
                    print = "x < y";
                    break;
                case SyntaxCode.SYNTAX_LOGIC_LESS_THAN_OR_EQUAL:
                    print = "x <= y";
                    break;
                case SyntaxCode.SYNTAX_LOGIC_OR:
                    print = "OR";

                    var rightChildCount2 = syntax.Storage.ChildCount - syntax.Storage.LeftChildCount;
                    print = $"{print}, left child count: {syntax.Storage.LeftChildCount}, right child count: {rightChildCount2}";

                    break;
                case SyntaxCode.SYNTAX_LOGIC_AND:
                    print = "AND";

                    var rightChildCount3 = syntax.Storage.ChildCount - syntax.Storage.LeftChildCount;
                    print = $"{print}, left child count: {syntax.Storage.LeftChildCount}, right child count: {rightChildCount3}";

                    break;

                default:

                    break;
            }

            Console.WriteLine(print);
        }
    }

    public static List<Syntaxio> Parse(ReadOnlySpan<char> content)
    {
        var handler    = default(LexerHandler);
        handler.Base   = ref MemoryMarshal.GetReference(content);
        handler.Length = content.Length;

        var syntaxes = new List<Syntaxio>(128);
        if(NextToken(out var token, ref handler))
        {
            do
            {
                if (token.TokenType == TokenType.TOKEN_SELECT)
                {
                    ParseSelect(syntaxes, ref handler);
                }

                token = default;
            } while (NextToken(out token, ref handler));
        }

        return syntaxes;
    }


    public static void ParseGeneral(List<Syntaxio> syntaxios, ref LexerHandler handler)
    {
        if (!PeekToken(out var token, ref handler))
        {
            return;
        }

        switch (token.TokenType)
        {
            case TokenType.TOKEN_SELECT:
                Advance(ref handler, token.Length);
                ParseSelect(syntaxios, ref handler);
                break;
        }
    }

    public static void ParseSelect(List<Syntaxio> syntaxios, ref LexerHandler handler)
    {
        // expecting list of columns
        // after that expecting "from"
        // after expecing [ identifier, (select expr) ]

        var selectIndex = syntaxios.Count;
        syntaxios.Add(new() { SyntaxCode = SyntaxCode.SYNTAX_SELECT });

        ParseColumnsEnumeration(syntaxios, ref handler);

        do
        {
            if (!PeekToken(out Token nextToken, ref handler))
            {
                break;
            }
            else if (nextToken.TokenType == TokenType.TOKEN_FROM)
            {
                Advance(ref handler, nextToken.Length);

                ParseFromSyntax(syntaxios, ref handler);

                // select [columns] from [identifier]
                if(!PeekToken(out nextToken, ref handler))
                {
                    break;
                }
            }
            else
            {
                // todo: error report
                break;
            }

            if(nextToken.TokenType == TokenType.TOKEN_WHERE)
            {
                Advance(ref handler, nextToken.Length);

                var whereIndex = syntaxios.Count;
                syntaxios.Add(new() { SyntaxCode = SyntaxCode.SYNTAX_WHERE });

                var isBooleanExpression = ParseBooleanExpression(syntaxios, ref handler, whereIndex + 1);
                if(!isBooleanExpression)
                {
                    // todo: error report
                    return;
                }

                SetChildCount(syntaxios, whereIndex);

                if (!PeekToken(out nextToken, ref handler))
                {
                    break;
                }
            }

            if (nextToken.TokenType == TokenType.TOKEN_ORDER)
            {
                Advance(ref handler, nextToken.Length);
                if(PeekToken(out nextToken, ref handler) && nextToken.TokenType == TokenType.TOKEN_BY)
                {
                    Advance(ref handler, nextToken.Length);

                    var orderBySyntaxIndex = syntaxios.Count;
                    syntaxios.Add(new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_ORDER_BY_ASCENDING });

                    ParseColumnsEnumeration(syntaxios, ref handler);
                    if(PeekToken(out nextToken, ref handler)
                        && nextToken.TokenType
                            is TokenType.TOKEN_ASCEDNING
                            or TokenType.TOKEN_DESCENDING)
                    {
                        Advance(ref handler, nextToken.Length);

                        if(nextToken.TokenType == TokenType.TOKEN_DESCENDING)
                        {
                            SetSyntaxCode(syntaxios, orderBySyntaxIndex, SyntaxCode.SYNTAX_ORDER_BY_DESCENDING);
                        }
                    }

                    SetChildCount(syntaxios, orderBySyntaxIndex);
                }
            }
        } while (false);


        SetChildCount(syntaxios, selectIndex);
    }

    public static void ParseColumnsEnumeration(List<Syntaxio> syntaxios, ref LexerHandler handler)
    {
        do
        {
            ParseSelectColumn(syntaxios, ref handler);

            if (!PeekToken(out var token, ref handler) || token.TokenType != TokenType.TOKEN_COMMA)
            {
                break;
            }

            Advance(ref handler, token.Length);

        } while (true);
    }

    public static void ParseSelectColumn(List<Syntaxio> syntaxios, ref LexerHandler handler)
    {
        if (!PeekToken(out var token, ref handler))
        {
            // todo: error report
            return;
        }

        // todo: check if renamed
        switch (token.TokenType)
        {
            case TokenType.TOKEN_IDENTIFIER:
            case TokenType.LITERAL_INT:
                ParseIdentifierOrExpression(syntaxios, ref handler);
                break;

            case TokenType.TOKEN_STAR:
                Advance(ref handler, token.Length);
                syntaxios.Add(new() { SyntaxCode = SyntaxCode.SYNTAX_COLUMNS_ALL });
                break;

            case TokenType.LITERAL_STRING:
                // todo: implement string arithmetics
                break;

            case TokenType.PAREN_OPEN:
                Advance(ref handler, token.Length);

                if (!PeekToken(out var framedToken, ref handler))
                {
                    // todo: error report
                    return;
                }

                syntaxios.Add(new() { SyntaxCode = SyntaxCode.SYNTAX_PARENTHESIZED_EXPRESSION });

                if (framedToken.TokenType
                    is TokenType.TOKEN_IDENTIFIER
                    or TokenType.LITERAL_INT)
                {
                    ParseIdentifierOrExpression(syntaxios, ref handler);
                }
                else if (framedToken.TokenType == TokenType.TOKEN_SELECT)
                {
                    ParseSelect(syntaxios, ref handler);
                    if (!PeekToken(out token, ref handler) || token.TokenType != TokenType.PAREN_CLOSE)
                    {
                        // todo: error report
                        return;
                    }
                }
                else
                {
                    // todo: error report
                    return;
                }

                break;
        }

    }

    public static void ParseFromSyntax(List<Syntaxio> syntaxios, ref LexerHandler handler)
    {
        var fromIndex = syntaxios.Count;
        syntaxios.Add(new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_FROM });

        var destinationIndex = syntaxios.Count;

        if(PeekToken(out var token, ref handler))
        {
            if(token.TokenType == TokenType.TOKEN_IDENTIFIER)
            {
                ParseIdentifierOrAccessExpression(syntaxios, ref handler, syntaxios.Count);
            }
            else if(token.TokenType == TokenType.PAREN_OPEN)
            {
                Advance(ref handler, token.Length);
                ParseSelect(syntaxios, ref handler);

                if(PeekToken(out token, ref handler))
                {
                    if(token.TokenType == TokenType.PAREN_CLOSE)
                    {
                        Advance(ref handler, token.Length);
                        if(token.TokenType == TokenType.TOKEN_IDENTIFIER)
                        {
                            var renamedSyntax = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_RENAMED_IDENTIFIER };
                            SetFramedToken(ref renamedSyntax, ref handler, ref token);
                            syntaxios.Insert(destinationIndex, renamedSyntax);
                        }
                        else
                        {
                            // todo: error report
                        }
                    }
                    else
                    {
                        // todo: error report
                    }
                }
                else
                {
                    // todo: error report
                }
            }
            else
            {
                // todo: error report
            }
        }
        else
        {
            // todo: error report
        }

        SetChildCount(syntaxios, fromIndex);
    }

    public static void ParseIdentifierOrAccessExpression(List<Syntaxio> syntaxios, ref LexerHandler handler, int destinationIndex)
    {
        if (PeekToken(out var nextToken, ref handler) && nextToken.TokenType == TokenType.TOKEN_IDENTIFIER)
        {
            Advance(ref handler, nextToken.Length);

            var identifierSyntaxInitial = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_IDENTIFIER };
            SetFramedToken(ref identifierSyntaxInitial, ref handler, ref nextToken);

            syntaxios.Add(identifierSyntaxInitial);

            while (PeekToken(out nextToken, ref handler) && nextToken.TokenType == TokenType.TOKEN_DOT)
            {
                Advance(ref handler, nextToken.Length);

                if (!PeekToken(out nextToken, ref handler) || nextToken.TokenType != TokenType.TOKEN_IDENTIFIER)
                {
                    // todo: error report
                    return;
                }

                Advance(ref handler, nextToken.Length);

                {
                    var accessSyntax = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_EXPRESSION_ACCESS };

                    accessSyntax.Storage.LeftChildCount = syntaxios.Count - destinationIndex;
                    accessSyntax.Storage.ChildCount     = syntaxios.Count - destinationIndex + 1;

                    syntaxios.Insert(destinationIndex, accessSyntax);
                }


                {
                    var identifierSyntax = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_IDENTIFIER };
                    SetFramedToken(ref identifierSyntax, ref handler, ref nextToken);

                    syntaxios.Add(identifierSyntax);
                }
            }
        }
    }

    public static void ParseIdentifierOrExpression(List<Syntaxio> syntaxios, ref LexerHandler handler)
    {
        var destinationIndex = syntaxios.Count;
        if(PeekToken(out var token, ref handler)
            && token.TokenType
                is TokenType.TOKEN_IDENTIFIER
                or TokenType.LITERAL_INT)
        {
            if(token.TokenType == TokenType.TOKEN_IDENTIFIER)
            {
                ParseIdentifierOrAccessExpression(syntaxios, ref handler, destinationIndex);
            }
            else if(token.TokenType == TokenType.LITERAL_INT)
            {
                var intSyntax = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_CONST_INT };

                intSyntax.Storage.Number =
                    int.Parse(MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref handler.Base, handler.Index), token.Length));

                Advance(ref handler, token.Length);

                syntaxios.Add(intSyntax);
            }

            if (!PeekToken(out var nextToken, ref handler))
            {
                return;
            }
            
            if(nextToken.TokenType
                is TokenType.TOKEN_PLUS
                or TokenType.TOKEN_MINUS
                or TokenType.TOKEN_STAR
                or TokenType.TOKEN_DIVIDE
                or TokenType.TOKEN_MODULO)
            {
                ParseExpression(syntaxios, ref handler, destinationIndex);
                if(!PeekToken(out nextToken, ref handler))
                {
                    // todo: error?
                    return;
                }
            }
            else if(nextToken.TokenType
                is TokenType.TOKEN_GREATER_THAN
                or TokenType.TOKEN_GREATER_OR_EQUAL
                or TokenType.TOKEN_LESS_THAN
                or TokenType.TOKEN_LESS_THAN_OR_EQUAL
                or TokenType.TOKEN_AND
                or TokenType.TOKEN_OR
                or TokenType.TOKEN_EQUAL
                or TokenType.TOKEN_NOT_EQUAL
                or TokenType.TOKEN_IN)
            {
                ParseBooleanExpression(syntaxios, ref handler, destinationIndex);
                if(!PeekToken(out nextToken, ref handler))
                {
                    // todo: error?
                    return;
                }
            }

            if (nextToken.TokenType == TokenType.TOKEN_IDENTIFIER)
            {
                Advance(ref handler, nextToken.Length);

                var reidentifier = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_RENAMED_IDENTIFIER };
                SetFramedToken(ref reidentifier, ref handler, ref token);

                syntaxios.Insert(destinationIndex, reidentifier);
                SetChildCount(syntaxios, destinationIndex);
            }
        }
    }

    public static bool ParseBooleanExpression(List<Syntaxio> syntaxios, ref LexerHandler handler, int destinationIndex)
    {
        var leftIsBoolean = ParseBooleanExpressionPrecedence2(syntaxios, ref handler, destinationIndex);
        if(!leftIsBoolean)
        {
            // todo: error report
            return false;
        }

        if (PeekToken(out var token, ref handler)
            && token.TokenType
                is TokenType.TOKEN_AND
                or TokenType.TOKEN_OR)
        {
            Advance(ref handler, token.Length);

            var syntaxCode = token.TokenType switch
            {
                TokenType.TOKEN_OR  => SyntaxCode.SYNTAX_LOGIC_OR,
                _ or
                TokenType.TOKEN_AND => SyntaxCode.SYNTAX_LOGIC_AND,
            };

            {
                var syntax = new Syntaxio() { SyntaxCode = syntaxCode };
                syntax.Storage.LeftChildCount = syntaxios.Count - destinationIndex;
                syntaxios.Insert(destinationIndex, syntax);
            }

            ParseBooleanExpressionPrecedence2(syntaxios, ref handler, syntaxios.Count);

            SetChildCount(syntaxios, destinationIndex);

            ParseBooleanExpression(syntaxios, ref handler, destinationIndex);

            return true;
        }

        return leftIsBoolean;
    }

    public static bool ParseBooleanExpressionPrecedence2(List<Syntaxio> syntaxios, ref LexerHandler handler, int destinationIndex)
    {
        ParseExpression(syntaxios, ref handler, syntaxios.Count);
        if (PeekToken(out var token, ref handler)
            && token.TokenType
                is TokenType.TOKEN_EQUAL
                or TokenType.TOKEN_GREATER_THAN
                or TokenType.TOKEN_GREATER_OR_EQUAL
                or TokenType.TOKEN_LESS_THAN_OR_EQUAL
                or TokenType.TOKEN_LESS_THAN
                or TokenType.TOKEN_IN)
        {
            Advance(ref handler, token.Length);

            var syntaxCode = token.TokenType switch
            {
                TokenType.TOKEN_GREATER_THAN       => SyntaxCode.SYNTAX_LOGIC_GREATER_THAN,
                TokenType.TOKEN_GREATER_OR_EQUAL   => SyntaxCode.SYNTAX_LOGIC_GREATER_THAN_OR_EQUAL,
                TokenType.TOKEN_LESS_THAN_OR_EQUAL => SyntaxCode.SYNTAX_LOGIC_LESS_THAN_OR_EQUAL,
                TokenType.TOKEN_LESS_THAN          => SyntaxCode.SYNTAX_LOGIC_LESS_THAN,
                TokenType.TOKEN_IN                 => SyntaxCode.SYNTAX_LOGIC_IN,
                TokenType.TOKEN_EQUAL or _         => SyntaxCode.SYNTAX_LOGIC_EQUAL,
            };

            {
                var syntax = new Syntaxio() { SyntaxCode = syntaxCode };
                syntax.Storage.LeftChildCount = syntaxios.Count - destinationIndex;
                syntaxios.Insert(destinationIndex, syntax);
            }

            ParseExpression(syntaxios, ref handler, syntaxios.Count);

            SetChildCount(syntaxios, destinationIndex);

            ParseBooleanExpressionPrecedence2(syntaxios, ref handler, destinationIndex);

            return true;
        }

        return false;
    }

    public static void ParseExpression(List<Syntaxio> syntaxios, ref LexerHandler handler, int destinationIndex)
    {
        ParseExpressionPrecedence2(syntaxios, ref handler, syntaxios.Count);
        if (PeekToken(out var token, ref handler)
            && token.TokenType is TokenType.TOKEN_PLUS
                or TokenType.TOKEN_MINUS)
        {
            Advance(ref handler, token.Length);

            var syntaxCode = token.TokenType == TokenType.TOKEN_PLUS
                ? SyntaxCode.SYNTAX_ADD
                : SyntaxCode.SYNTAX_SUBSTRACT;

            {
                var syntax = new Syntaxio() { SyntaxCode = syntaxCode };
                syntax.Storage.LeftChildCount = syntaxios.Count - destinationIndex;
                syntaxios.Insert(destinationIndex, syntax);
            }

            ParseExpressionPrecedence2(syntaxios, ref handler, syntaxios.Count);

            SetChildCount(syntaxios, destinationIndex);

            ParseExpression(syntaxios, ref handler, destinationIndex);
        }
    }

    public static void ParseExpressionPrecedence2(List<Syntaxio> syntaxios, ref LexerHandler handler, int destinationIndex)
    {
        ParsePrimary(syntaxios, ref handler);
        
        if (PeekToken(out var token, ref handler)
            && token.TokenType is TokenType.TOKEN_STAR
                or TokenType.TOKEN_DIVIDE
                or TokenType.TOKEN_MODULO)
        {
            Advance(ref handler, token.Length);

            var syntaxCode = token.TokenType switch
            {
                TokenType.TOKEN_STAR   => SyntaxCode.SYNTAX_MULTIPLY,
                TokenType.TOKEN_DIVIDE => SyntaxCode.SYNTAX_DIVIDE,
                _ => SyntaxCode.SYNTAX_MODULO,
            };
            var leftConst = 0;

            bool isLeftConstInt = syntaxios[^1].SyntaxCode == SyntaxCode.SYNTAX_CONST_INT;
            if (isLeftConstInt)
            {
                leftConst = syntaxios[^1].Storage.Number;
            }

            {
                var syntax = new Syntaxio() { SyntaxCode = syntaxCode };
                syntax.Storage.LeftChildCount = syntaxios.Count - destinationIndex;
                syntaxios.Insert(destinationIndex, syntax);
            }

            ParsePrimary(syntaxios, ref handler);


            if (isLeftConstInt && syntaxios[^1].SyntaxCode == SyntaxCode.SYNTAX_CONST_INT)
            {
                var rightConst     = syntaxios[^1].Storage.Number;
                var constEvaluated = token.TokenType switch
                {
                    TokenType.TOKEN_STAR   => leftConst * rightConst,
                    TokenType.TOKEN_DIVIDE => leftConst / rightConst, _ or
                    TokenType.TOKEN_MODULO => leftConst % rightConst,
                };

                var constEvaluatedSyntax = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_CONST_INT };
                constEvaluatedSyntax.Storage.Number = constEvaluated;

                syntaxios[destinationIndex] = constEvaluatedSyntax;
                syntaxios.RemoveRange(syntaxios.Count - 2, 2);
            }
            else
            {
                SetChildCount(syntaxios, destinationIndex);
            }

            ParseExpressionPrecedence2(syntaxios, ref handler, syntaxios.Count);
        }
    }

    public static void ParsePrimary(List<Syntaxio> syntaxios, ref LexerHandler handler)
    {
        if(!PeekToken(out var token, ref handler))
        {
            return;
        }

        switch(token.TokenType)
        {
            case TokenType.PAREN_OPEN:
                Advance(ref handler, token.Length);

                syntaxios.Add(new() { SyntaxCode = SyntaxCode.SYNTAX_PAREN_OPEN });

                if(!PeekToken(out var nextToken, ref handler))
                {
                    // todo: error?
                    return;
                }
                else if(nextToken.TokenType == TokenType.TOKEN_SELECT)
                {
                    Advance(ref handler, nextToken.Length);

                    // todo: add restrictions for sub-queries
                    ParseSelect(syntaxios, ref handler);
                }
                else if(nextToken.TokenType == TokenType.LITERAL_STRING)
                {
                    var enumerateSyntax = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_CONST_STRING_ENUMERATION };
                    var destinationIndex = syntaxios.Count;

                    syntaxios.Add(enumerateSyntax);

                    ParseEnumeration(syntaxios, ref handler, ref enumerateSyntax.Storage.ChildCount, TokenType.LITERAL_STRING, SyntaxCode.SYNTAX_CONST_STRING);

                    syntaxios[destinationIndex] = enumerateSyntax;
                }
                else if(nextToken.TokenType == TokenType.LITERAL_INT)
                {
                    var enumerateSyntax = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_CONST_INT_ENUMERATION };
                    var destinationIndex = syntaxios.Count;

                    syntaxios.Add(enumerateSyntax);
                    
                    ParseEnumeration(syntaxios, ref handler, ref enumerateSyntax.Storage.ChildCount, TokenType.LITERAL_INT, SyntaxCode.SYNTAX_CONST_INT);

                    syntaxios[destinationIndex] = enumerateSyntax;
                }
                else
                {
                    // @FixMe: only assumes for where ... (bool)
                    Advance(ref handler, nextToken.Length);
                    var isBooleanExpression = ParseBooleanExpression(syntaxios, ref handler, syntaxios.Count);
                    if(!isBooleanExpression)
                    {
                        // todo: error report
                    }
                }

                //ParsePrimary(syntaxios, ref handler);

                if (syntaxios[^1].SyntaxCode != SyntaxCode.SYNTAX_PAREN_CLOSE)
                {
                    // todo: error report
                    return;
                }
                break;

            case TokenType.PAREN_CLOSE:
                Advance(ref handler, token.Length);
                syntaxios.Add(new() { SyntaxCode = SyntaxCode.SYNTAX_PAREN_CLOSE });
                break;
            case TokenType.LITERAL_STRING:
                Advance(ref handler, token.Length);

                var stringSyntax = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_CONST_STRING };
                SetFramedToken(ref stringSyntax, ref handler, ref token);

                syntaxios.Add(stringSyntax);

                break;

            case TokenType.LITERAL_INT:
                var intSyntax = new Syntaxio() { SyntaxCode = SyntaxCode.SYNTAX_CONST_INT };

                intSyntax.Storage.Number =
                    int.Parse(MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref handler.Base, handler.Index), token.Length));

                Advance(ref handler, token.Length);

                syntaxios.Add(intSyntax);
                break;
            case TokenType.TOKEN_IDENTIFIER:
                ParseIdentifierOrAccessExpression(syntaxios, ref handler, syntaxios.Count);
                
                //ParseIdentifierOrExpression(syntaxios, ref handler);
                break;
        }
    }

    public static void ParseEnumeration(
        List<Syntaxio> syntaxios,
        ref LexerHandler handler,
        ref int count,
        TokenType enumeratedTokens,
        SyntaxCode syntaxCode)
    {
        if(PeekToken(out var token, ref handler))
        {
            while (true)
            {
                count ++;

                Advance(ref handler, token.Length);

                var stringSyntax = new Syntaxio() { SyntaxCode = syntaxCode };

                if(enumeratedTokens == TokenType.LITERAL_INT)
                {
                    var rawStringInteger = MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref handler.Base, token.Start), token.Length);
                    stringSyntax.Storage.Number = int.Parse(rawStringInteger);
                }
                else
                {
                    SetFramedToken(ref stringSyntax, ref handler, ref token);
                }

                syntaxios.Add(stringSyntax);

                if(!PeekToken(out token, ref handler))
                {
                    // todo: error report
                    return;
                }
                else if(token.TokenType == TokenType.PAREN_CLOSE)
                {
                    syntaxios.Add(new() { SyntaxCode = SyntaxCode.SYNTAX_PAREN_CLOSE });
                    return;
                }
                else if(token.TokenType != TokenType.TOKEN_COMMA)
                {
                    // todo: error report
                    return;
                }

                Advance(ref handler, token.Length);
                if (!PeekToken(out token, ref handler))
                {
                    // todo: error report
                    return;
                }
                else if(token.TokenType != enumeratedTokens)
                {
                    // todo: error report
                    return;
                }
            }
        }
    }

    public static void SetFramedToken(ref Syntaxio syntaxio, ref LexerHandler handler, ref Token token)
    {
        syntaxio.Storage.StringStart  = handler.Index - token.Length;
        syntaxio.Storage.StringLength = token.Length;
    }

    public static void SetChildCount(List<Syntaxio> syntaxios, int index)
    {
        var syntax = syntaxios[index];
        syntax.Storage.ChildCount = syntaxios.Count - 1 - index; // note: diff as indexes
        syntaxios[index] = syntax;
    }

    public static void SetSyntaxCode(List<Syntaxio> syntaxios, int index, SyntaxCode code)
    {
        var syntax = syntaxios[index];
        syntax.SyntaxCode = code;
        syntaxios[index] = syntax;
    }

    public static List<Token> GetTokens(ReadOnlySpan<char> content)
    {
        var tokens = new List<Token>();

        ref char c = ref MemoryMarshal.GetReference(content);

        var handler    = default(LexerHandler);
        handler.Base   = ref c;
        handler.Length = content.Length;

        var token = default(Token);

        while(NextToken(out token, ref handler))
        {
            tokens.Add(token);
            token = default;
        }

        return tokens;
    }

    public static bool NextToken(out Token token, ref LexerHandler handler)
    {
        EatSpaces(ref handler);

        var anyToken   = PeekToken(out token, ref handler);
        handler.Index += token.Length;

        return anyToken;
    }

    public static void Advance(ref LexerHandler handler, int length)
    {
        handler.Index += length;
    }

    public static bool PeekToken(out Token token, ref LexerHandler handler)
    {
        EatSpaces(ref handler);

        token = default;

        var remainingLength = handler.Length - handler.Index;
        if (remainingLength <= 0)
        {
            return false;
        }

        token.Start = handler.Index;

        var span = MemoryMarshal.CreateReadOnlySpan(
            ref Unsafe.Add(ref handler.Base, handler.Index),
            remainingLength
        );

        token.TokenType = TokenType.ERROR;

        if (IsPreservedKeyword(span, ref token))
        { }
        else if (IsSingleCharToken(span, ref token))
        { }
        else if (IsFramedToken(span, ref token))
        { }
        else if (IsNumberLiteral(span, ref token))
        { }
        else if (IsIdentifier(span, ref token))
        { }
        else
        {
            var errorTokenLength = 0;
            while (!IsEndOrWhiteSpaceOrSeparator(span, errorTokenLength))
            {
                errorTokenLength++;
                handler.Index++;
            }

            token.Length = errorTokenLength;
            token.TokenType = TokenType.ERROR;
        }

        return true;
    }

    public static bool IsSingleCharToken(ReadOnlySpan<char> span, ref Token token)
    {
        if (span.Length <= 0)
        {
            return false;
        }

        var tokens     = SingleCharTokens;
        var singleChar = span[0];

        var index = tokens.IndexOf(singleChar);

        if(index >= 0)
        {
            token.TokenType = SingleCharTokenTypes[index];
            token.Length    = 1;
        }

        return index >= 0;
    }

    public static bool IsPreservedKeyword(ReadOnlySpan<char> span, ref Token token)
    {
        if(span.Length <= 0)
        {
            return false;
        }

        var preserved = Preserved;

        for(var i = 0; i < preserved.Length; i++)
        {
            var (tokenText, tokenType) = preserved[i];

            if (span.StartsWith(tokenText, StringComparison.OrdinalIgnoreCase))
            {
                if(IsEndOrWhiteSpaceOrSeparator(span, tokenText.Length))
                {
                    token.Length    = tokenText.Length;
                    token.TokenType = tokenType;

                    return true;
                }
            }
        }

        return false;
    }

    public static bool IsFramedToken(ReadOnlySpan<char> span, ref Token token)
    {
        if (span.Length <= 0)
        {
            return false;
        }

        var openChar = span[0];
        var index = FramingTokens.IndexOf(openChar);

        if(index < 0)
        {
            return false;
        }

        var endChar           = FramingTokenClosingCharTable[index];
        var tokenTypeIfClosed = FramingTokenTypeTable[index];

        var tokenLength = 0;
        var lastChar    = default(char);
        
        var notEnded = true;
        while (notEnded)
        {
            lastChar = span[tokenLength];

            tokenLength  ++;

            if (tokenLength >= span.Length) notEnded = false;
            // note: ]] and '' - escaping
            else if (span[tokenLength] == endChar)
            {
                lastChar = span[tokenLength];

                if (tokenLength + 1 >= span.Length)        notEnded = false;
                else if (span[tokenLength + 1] != endChar) notEnded = false;

                tokenLength ++;
            }
        }

        token.Length    = tokenLength;
        token.TokenType = tokenTypeIfClosed;

        if (lastChar != endChar)
        {
            token.TokenType = FramingTokenMissingClosingTypeTable[index];
        }

        return true;
    }

    public static bool IsNumberLiteral(ReadOnlySpan<char> span, ref Token token)
    {
        var numberLength = 0;

        while(numberLength < span.Length
            && char.IsNumber(span[numberLength]))
        {
            numberLength ++;
        }

        var isNumber = true;

        if(numberLength == 0)                                      isNumber = false;
        else if(!IsEndOrWhiteSpaceOrSeparator(span, numberLength)) isNumber = false;

        if(isNumber)
        {
            token.Length    = numberLength;
            token.TokenType = TokenType.LITERAL_INT;
        }

        return isNumber;
    }

    public static bool IsIdentifier(ReadOnlySpan<char> span, ref Token token)
    {
        var identifierLength = 0;

        while (identifierLength < span.Length
            && char.IsLetterOrDigit(span[identifierLength]))
        {
            identifierLength ++;
        }

        var isIdentifier = true;

        if(identifierLength == 0)                                      isIdentifier = false;
        else if(!IsEndOrWhiteSpaceOrSeparator(span, identifierLength)) isIdentifier = false;

        if(isIdentifier)
        {
            token.Length    = identifierLength;
            token.TokenType = TokenType.TOKEN_IDENTIFIER;
        }

        return isIdentifier;
    }

    public static bool IsEndOrWhiteSpaceOrSeparator(ReadOnlySpan<char> span, int index)
    {
        if (index >= span.Length)
            return true;

        return char.IsWhiteSpace(span[index]) || SingleCharTokens.IndexOf(span[index]) >= 0;
    }

    public static void EatSpaces(ref LexerHandler handler)
    {
        while(
            handler.Index < handler.Length
            && char.IsWhiteSpace(
                Unsafe.Add(ref handler.Base, handler.Index)
            )
        )
        {
            handler.Index ++;
        }
    }
}

public ref struct LexerHandler
{
    public ref char Base;
    public int Length;
    public int Index;
}

public struct Token
{
    public TokenType TokenType;
    public int Start;
    public int Length;
}

public enum TokenType
{
    ERROR,
    TOKEN_SELECT,
    TOKEN_COMMA,
    TOKEN_DOT,
    TOKEN_TOP,
    TOKEN_STAR,
    TOKEN_UPDATE,
    TOKEN_DELETE,
    TOKEN_FROM,
    JOIN_MODIFIER_INNER,
    JOIN_MODIFIER_LEFT,
    JOIN_MODIFIER_RIGHT,
    TOKNE_JOIN,
    TOKEN_WHERE,
    TOKEN_AND,
    TOKEN_NOT,
    TOKEN_OR,
    TOKEN_IN,
    PAREN_OPEN,
    PAREN_CLOSE,
    TOKEN_IDENTIFIER,
    TOKEN_ORDER,
    TOKEN_GROUP,
    TOKEN_BY,
    TOKEN_ASCEDNING,
    TOKEN_DESCENDING,
    LITERAL_INT,
    LITERAL_STRING,
    LITERAL_DATE,
    LITERAL_FLOAT,
    QUOTE_SINGLE,
    QUOTE_DOUBLE,
    TOKEN_GREATER_THAN,
    TOKEN_GREATER_OR_EQUAL,
    TOKEN_LESS_THAN,
    TOKEN_LESS_THAN_OR_EQUAL,
    TOKEN_EQUAL,
    TOKEN_NOT_EQUAL,
    TOKEN_PLUS,
    TOKEN_MINUS,
    TOKEN_DIVIDE,
    TOKEN_MODULO,
    TOKEN_BITWISE_OR,
    TOKEN_BITWISE_AND,
    TOKEN_BITWISE_XOR,
    TOKEN_BITWISE_NOT,
    TOKEN_MISSING_BRACKET_END,
    TOKEN_MISSING_QUOTE_END,
    TOKEN_MISSING_DOUBLE_QUOTE_END
}

public struct Syntaxio
{
    public SyntaxCode SyntaxCode;
    public SyntaxStorage Storage;
}

[StructLayout(LayoutKind.Explicit, Size = 12)]
public struct SyntaxStorage
{
    [FieldOffset(0)]
    public int Number;

    [FieldOffset(0)]
    public int StringStart;
    [FieldOffset(4)]
    public int StringLength;

    [FieldOffset(0)]
    public int LeftChildCount;

    [FieldOffset(8)]
    public int ChildCount;
}

public enum SyntaxCode
{
    SYNTAX_SELECT,
    SYNTAX_UPDATE,
    SYNTAX_DELETE,
    SYNTAX_WHERE,

    SYNTAX_FROM,

    SYNTAX_ERROR,

    SYNTAX_ORDER_BY_ASCENDING,
    SYNTAX_ORDER_BY_DESCENDING,

    SYNTAX_COLUMNS_ALL,
    SYNTAX_IDENTIFIER,
    SYNTAX_RENAMED_IDENTIFIER,

    SYNTAX_PARENTHESIZED_EXPRESSION,
    SYNTAX_PAREN_OPEN,
    SYNTAX_PAREN_CLOSE,

    SYNTAX_ADD,
    SYNTAX_SUBSTRACT,
    SYNTAX_MULTIPLY,
    SYNTAX_DIVIDE,
    SYNTAX_MODULO,

    SYNTAX_CONST_INT,
    SYNTAX_CONST_STRING,

    SYNTAX_CONST_INT_ENUMERATION,
    SYNTAX_CONST_STRING_ENUMERATION,

    SYNTAX_EXPRESSION_ACCESS,

    SYNTAX_LOGIC_EQUAL,
    SYNTAX_LOGIC_NOT_EQUAL,
    SYNTAX_LOGIC_GREATER_THAN,
    SYNTAX_LOGIC_GREATER_THAN_OR_EQUAL,
    SYNTAX_LOGIC_LESS_THAN,
    SYNTAX_LOGIC_LESS_THAN_OR_EQUAL,
    SYNTAX_LOGIC_IN,
    SYNTAX_LOGIC_OR,
    SYNTAX_LOGIC_AND,
}

















internal class Example
{
    public string name;
    public int id;
    public int age;

    public override string ToString()
    {
        return $"id - {id}, age - {age}, name - \"{name}\"";
    }
}