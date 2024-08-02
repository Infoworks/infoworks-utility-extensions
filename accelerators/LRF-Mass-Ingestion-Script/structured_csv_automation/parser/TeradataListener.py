# Generated from ./grammar/Teradata.g4 by ANTLR 4.9
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .TeradataParser import TeradataParser
else:
    from TeradataParser import TeradataParser

# This class defines a complete listener for a parse tree produced by TeradataParser.
class TeradataListener(ParseTreeListener):

    # Enter a parse tree produced by TeradataParser#queryCallback.
    def enterQueryCallback(self, ctx:TeradataParser.QueryCallbackContext):
        pass

    # Exit a parse tree produced by TeradataParser#queryCallback.
    def exitQueryCallback(self, ctx:TeradataParser.QueryCallbackContext):
        pass


    # Enter a parse tree produced by TeradataParser#createCallback.
    def enterCreateCallback(self, ctx:TeradataParser.CreateCallbackContext):
        pass

    # Exit a parse tree produced by TeradataParser#createCallback.
    def exitCreateCallback(self, ctx:TeradataParser.CreateCallbackContext):
        pass


    # Enter a parse tree produced by TeradataParser#insertCallback.
    def enterInsertCallback(self, ctx:TeradataParser.InsertCallbackContext):
        pass

    # Exit a parse tree produced by TeradataParser#insertCallback.
    def exitInsertCallback(self, ctx:TeradataParser.InsertCallbackContext):
        pass


    # Enter a parse tree produced by TeradataParser#updateCallback.
    def enterUpdateCallback(self, ctx:TeradataParser.UpdateCallbackContext):
        pass

    # Exit a parse tree produced by TeradataParser#updateCallback.
    def exitUpdateCallback(self, ctx:TeradataParser.UpdateCallbackContext):
        pass


    # Enter a parse tree produced by TeradataParser#deleteCallback.
    def enterDeleteCallback(self, ctx:TeradataParser.DeleteCallbackContext):
        pass

    # Exit a parse tree produced by TeradataParser#deleteCallback.
    def exitDeleteCallback(self, ctx:TeradataParser.DeleteCallbackContext):
        pass


    # Enter a parse tree produced by TeradataParser#dropCallback.
    def enterDropCallback(self, ctx:TeradataParser.DropCallbackContext):
        pass

    # Exit a parse tree produced by TeradataParser#dropCallback.
    def exitDropCallback(self, ctx:TeradataParser.DropCallbackContext):
        pass


    # Enter a parse tree produced by TeradataParser#createTable.
    def enterCreateTable(self, ctx:TeradataParser.CreateTableContext):
        pass

    # Exit a parse tree produced by TeradataParser#createTable.
    def exitCreateTable(self, ctx:TeradataParser.CreateTableContext):
        pass


    # Enter a parse tree produced by TeradataParser#columnsList.
    def enterColumnsList(self, ctx:TeradataParser.ColumnsListContext):
        pass

    # Exit a parse tree produced by TeradataParser#columnsList.
    def exitColumnsList(self, ctx:TeradataParser.ColumnsListContext):
        pass


    # Enter a parse tree produced by TeradataParser#tableProperties.
    def enterTableProperties(self, ctx:TeradataParser.TablePropertiesContext):
        pass

    # Exit a parse tree produced by TeradataParser#tableProperties.
    def exitTableProperties(self, ctx:TeradataParser.TablePropertiesContext):
        pass


    # Enter a parse tree produced by TeradataParser#tableIndex.
    def enterTableIndex(self, ctx:TeradataParser.TableIndexContext):
        pass

    # Exit a parse tree produced by TeradataParser#tableIndex.
    def exitTableIndex(self, ctx:TeradataParser.TableIndexContext):
        pass


    # Enter a parse tree produced by TeradataParser#query.
    def enterQuery(self, ctx:TeradataParser.QueryContext):
        pass

    # Exit a parse tree produced by TeradataParser#query.
    def exitQuery(self, ctx:TeradataParser.QueryContext):
        pass


    # Enter a parse tree produced by TeradataParser#transactions.
    def enterTransactions(self, ctx:TeradataParser.TransactionsContext):
        pass

    # Exit a parse tree produced by TeradataParser#transactions.
    def exitTransactions(self, ctx:TeradataParser.TransactionsContext):
        pass


    # Enter a parse tree produced by TeradataParser#insertInto.
    def enterInsertInto(self, ctx:TeradataParser.InsertIntoContext):
        pass

    # Exit a parse tree produced by TeradataParser#insertInto.
    def exitInsertInto(self, ctx:TeradataParser.InsertIntoContext):
        pass


    # Enter a parse tree produced by TeradataParser#update.
    def enterUpdate(self, ctx:TeradataParser.UpdateContext):
        pass

    # Exit a parse tree produced by TeradataParser#update.
    def exitUpdate(self, ctx:TeradataParser.UpdateContext):
        pass


    # Enter a parse tree produced by TeradataParser#updateSetExpression.
    def enterUpdateSetExpression(self, ctx:TeradataParser.UpdateSetExpressionContext):
        pass

    # Exit a parse tree produced by TeradataParser#updateSetExpression.
    def exitUpdateSetExpression(self, ctx:TeradataParser.UpdateSetExpressionContext):
        pass


    # Enter a parse tree produced by TeradataParser#updateCondition.
    def enterUpdateCondition(self, ctx:TeradataParser.UpdateConditionContext):
        pass

    # Exit a parse tree produced by TeradataParser#updateCondition.
    def exitUpdateCondition(self, ctx:TeradataParser.UpdateConditionContext):
        pass


    # Enter a parse tree produced by TeradataParser#delete.
    def enterDelete(self, ctx:TeradataParser.DeleteContext):
        pass

    # Exit a parse tree produced by TeradataParser#delete.
    def exitDelete(self, ctx:TeradataParser.DeleteContext):
        pass


    # Enter a parse tree produced by TeradataParser#deleteCondition.
    def enterDeleteCondition(self, ctx:TeradataParser.DeleteConditionContext):
        pass

    # Exit a parse tree produced by TeradataParser#deleteCondition.
    def exitDeleteCondition(self, ctx:TeradataParser.DeleteConditionContext):
        pass


    # Enter a parse tree produced by TeradataParser#dropTable.
    def enterDropTable(self, ctx:TeradataParser.DropTableContext):
        pass

    # Exit a parse tree produced by TeradataParser#dropTable.
    def exitDropTable(self, ctx:TeradataParser.DropTableContext):
        pass


    # Enter a parse tree produced by TeradataParser#ctes.
    def enterCtes(self, ctx:TeradataParser.CtesContext):
        pass

    # Exit a parse tree produced by TeradataParser#ctes.
    def exitCtes(self, ctx:TeradataParser.CtesContext):
        pass


    # Enter a parse tree produced by TeradataParser#namedQuery.
    def enterNamedQuery(self, ctx:TeradataParser.NamedQueryContext):
        pass

    # Exit a parse tree produced by TeradataParser#namedQuery.
    def exitNamedQuery(self, ctx:TeradataParser.NamedQueryContext):
        pass


    # Enter a parse tree produced by TeradataParser#tableProvider.
    def enterTableProvider(self, ctx:TeradataParser.TableProviderContext):
        pass

    # Exit a parse tree produced by TeradataParser#tableProvider.
    def exitTableProvider(self, ctx:TeradataParser.TableProviderContext):
        pass


    # Enter a parse tree produced by TeradataParser#queryNoWith.
    def enterQueryNoWith(self, ctx:TeradataParser.QueryNoWithContext):
        pass

    # Exit a parse tree produced by TeradataParser#queryNoWith.
    def exitQueryNoWith(self, ctx:TeradataParser.QueryNoWithContext):
        pass


    # Enter a parse tree produced by TeradataParser#queryOrganization.
    def enterQueryOrganization(self, ctx:TeradataParser.QueryOrganizationContext):
        pass

    # Exit a parse tree produced by TeradataParser#queryOrganization.
    def exitQueryOrganization(self, ctx:TeradataParser.QueryOrganizationContext):
        pass


    # Enter a parse tree produced by TeradataParser#queryTermDefault.
    def enterQueryTermDefault(self, ctx:TeradataParser.QueryTermDefaultContext):
        pass

    # Exit a parse tree produced by TeradataParser#queryTermDefault.
    def exitQueryTermDefault(self, ctx:TeradataParser.QueryTermDefaultContext):
        pass


    # Enter a parse tree produced by TeradataParser#setOperation.
    def enterSetOperation(self, ctx:TeradataParser.SetOperationContext):
        pass

    # Exit a parse tree produced by TeradataParser#setOperation.
    def exitSetOperation(self, ctx:TeradataParser.SetOperationContext):
        pass


    # Enter a parse tree produced by TeradataParser#queryPrimaryDefault.
    def enterQueryPrimaryDefault(self, ctx:TeradataParser.QueryPrimaryDefaultContext):
        pass

    # Exit a parse tree produced by TeradataParser#queryPrimaryDefault.
    def exitQueryPrimaryDefault(self, ctx:TeradataParser.QueryPrimaryDefaultContext):
        pass


    # Enter a parse tree produced by TeradataParser#table.
    def enterTable(self, ctx:TeradataParser.TableContext):
        pass

    # Exit a parse tree produced by TeradataParser#table.
    def exitTable(self, ctx:TeradataParser.TableContext):
        pass


    # Enter a parse tree produced by TeradataParser#inlineTableDefault1.
    def enterInlineTableDefault1(self, ctx:TeradataParser.InlineTableDefault1Context):
        pass

    # Exit a parse tree produced by TeradataParser#inlineTableDefault1.
    def exitInlineTableDefault1(self, ctx:TeradataParser.InlineTableDefault1Context):
        pass


    # Enter a parse tree produced by TeradataParser#subquery.
    def enterSubquery(self, ctx:TeradataParser.SubqueryContext):
        pass

    # Exit a parse tree produced by TeradataParser#subquery.
    def exitSubquery(self, ctx:TeradataParser.SubqueryContext):
        pass


    # Enter a parse tree produced by TeradataParser#sortItem.
    def enterSortItem(self, ctx:TeradataParser.SortItemContext):
        pass

    # Exit a parse tree produced by TeradataParser#sortItem.
    def exitSortItem(self, ctx:TeradataParser.SortItemContext):
        pass


    # Enter a parse tree produced by TeradataParser#querySpecification.
    def enterQuerySpecification(self, ctx:TeradataParser.QuerySpecificationContext):
        pass

    # Exit a parse tree produced by TeradataParser#querySpecification.
    def exitQuerySpecification(self, ctx:TeradataParser.QuerySpecificationContext):
        pass


    # Enter a parse tree produced by TeradataParser#fromClause.
    def enterFromClause(self, ctx:TeradataParser.FromClauseContext):
        pass

    # Exit a parse tree produced by TeradataParser#fromClause.
    def exitFromClause(self, ctx:TeradataParser.FromClauseContext):
        pass


    # Enter a parse tree produced by TeradataParser#whereClause.
    def enterWhereClause(self, ctx:TeradataParser.WhereClauseContext):
        pass

    # Exit a parse tree produced by TeradataParser#whereClause.
    def exitWhereClause(self, ctx:TeradataParser.WhereClauseContext):
        pass


    # Enter a parse tree produced by TeradataParser#havingClause.
    def enterHavingClause(self, ctx:TeradataParser.HavingClauseContext):
        pass

    # Exit a parse tree produced by TeradataParser#havingClause.
    def exitHavingClause(self, ctx:TeradataParser.HavingClauseContext):
        pass


    # Enter a parse tree produced by TeradataParser#qualifyClause.
    def enterQualifyClause(self, ctx:TeradataParser.QualifyClauseContext):
        pass

    # Exit a parse tree produced by TeradataParser#qualifyClause.
    def exitQualifyClause(self, ctx:TeradataParser.QualifyClauseContext):
        pass


    # Enter a parse tree produced by TeradataParser#aggregation.
    def enterAggregation(self, ctx:TeradataParser.AggregationContext):
        pass

    # Exit a parse tree produced by TeradataParser#aggregation.
    def exitAggregation(self, ctx:TeradataParser.AggregationContext):
        pass


    # Enter a parse tree produced by TeradataParser#groupingSet.
    def enterGroupingSet(self, ctx:TeradataParser.GroupingSetContext):
        pass

    # Exit a parse tree produced by TeradataParser#groupingSet.
    def exitGroupingSet(self, ctx:TeradataParser.GroupingSetContext):
        pass


    # Enter a parse tree produced by TeradataParser#lateralView.
    def enterLateralView(self, ctx:TeradataParser.LateralViewContext):
        pass

    # Exit a parse tree produced by TeradataParser#lateralView.
    def exitLateralView(self, ctx:TeradataParser.LateralViewContext):
        pass


    # Enter a parse tree produced by TeradataParser#setQuantifier.
    def enterSetQuantifier(self, ctx:TeradataParser.SetQuantifierContext):
        pass

    # Exit a parse tree produced by TeradataParser#setQuantifier.
    def exitSetQuantifier(self, ctx:TeradataParser.SetQuantifierContext):
        pass


    # Enter a parse tree produced by TeradataParser#relation.
    def enterRelation(self, ctx:TeradataParser.RelationContext):
        pass

    # Exit a parse tree produced by TeradataParser#relation.
    def exitRelation(self, ctx:TeradataParser.RelationContext):
        pass


    # Enter a parse tree produced by TeradataParser#joinRelation.
    def enterJoinRelation(self, ctx:TeradataParser.JoinRelationContext):
        pass

    # Exit a parse tree produced by TeradataParser#joinRelation.
    def exitJoinRelation(self, ctx:TeradataParser.JoinRelationContext):
        pass


    # Enter a parse tree produced by TeradataParser#joinType.
    def enterJoinType(self, ctx:TeradataParser.JoinTypeContext):
        pass

    # Exit a parse tree produced by TeradataParser#joinType.
    def exitJoinType(self, ctx:TeradataParser.JoinTypeContext):
        pass


    # Enter a parse tree produced by TeradataParser#joinCriteria.
    def enterJoinCriteria(self, ctx:TeradataParser.JoinCriteriaContext):
        pass

    # Exit a parse tree produced by TeradataParser#joinCriteria.
    def exitJoinCriteria(self, ctx:TeradataParser.JoinCriteriaContext):
        pass


    # Enter a parse tree produced by TeradataParser#sample.
    def enterSample(self, ctx:TeradataParser.SampleContext):
        pass

    # Exit a parse tree produced by TeradataParser#sample.
    def exitSample(self, ctx:TeradataParser.SampleContext):
        pass


    # Enter a parse tree produced by TeradataParser#identifierList.
    def enterIdentifierList(self, ctx:TeradataParser.IdentifierListContext):
        pass

    # Exit a parse tree produced by TeradataParser#identifierList.
    def exitIdentifierList(self, ctx:TeradataParser.IdentifierListContext):
        pass


    # Enter a parse tree produced by TeradataParser#identifierSeq.
    def enterIdentifierSeq(self, ctx:TeradataParser.IdentifierSeqContext):
        pass

    # Exit a parse tree produced by TeradataParser#identifierSeq.
    def exitIdentifierSeq(self, ctx:TeradataParser.IdentifierSeqContext):
        pass


    # Enter a parse tree produced by TeradataParser#orderedIdentifierList.
    def enterOrderedIdentifierList(self, ctx:TeradataParser.OrderedIdentifierListContext):
        pass

    # Exit a parse tree produced by TeradataParser#orderedIdentifierList.
    def exitOrderedIdentifierList(self, ctx:TeradataParser.OrderedIdentifierListContext):
        pass


    # Enter a parse tree produced by TeradataParser#orderedIdentifier.
    def enterOrderedIdentifier(self, ctx:TeradataParser.OrderedIdentifierContext):
        pass

    # Exit a parse tree produced by TeradataParser#orderedIdentifier.
    def exitOrderedIdentifier(self, ctx:TeradataParser.OrderedIdentifierContext):
        pass


    # Enter a parse tree produced by TeradataParser#identifierCommentList.
    def enterIdentifierCommentList(self, ctx:TeradataParser.IdentifierCommentListContext):
        pass

    # Exit a parse tree produced by TeradataParser#identifierCommentList.
    def exitIdentifierCommentList(self, ctx:TeradataParser.IdentifierCommentListContext):
        pass


    # Enter a parse tree produced by TeradataParser#identifierComment.
    def enterIdentifierComment(self, ctx:TeradataParser.IdentifierCommentContext):
        pass

    # Exit a parse tree produced by TeradataParser#identifierComment.
    def exitIdentifierComment(self, ctx:TeradataParser.IdentifierCommentContext):
        pass


    # Enter a parse tree produced by TeradataParser#relationPrimary.
    def enterRelationPrimary(self, ctx:TeradataParser.RelationPrimaryContext):
        pass

    # Exit a parse tree produced by TeradataParser#relationPrimary.
    def exitRelationPrimary(self, ctx:TeradataParser.RelationPrimaryContext):
        pass


    # Enter a parse tree produced by TeradataParser#inlineTable.
    def enterInlineTable(self, ctx:TeradataParser.InlineTableContext):
        pass

    # Exit a parse tree produced by TeradataParser#inlineTable.
    def exitInlineTable(self, ctx:TeradataParser.InlineTableContext):
        pass


    # Enter a parse tree produced by TeradataParser#valuesExpression.
    def enterValuesExpression(self, ctx:TeradataParser.ValuesExpressionContext):
        pass

    # Exit a parse tree produced by TeradataParser#valuesExpression.
    def exitValuesExpression(self, ctx:TeradataParser.ValuesExpressionContext):
        pass


    # Enter a parse tree produced by TeradataParser#tableIdentifier.
    def enterTableIdentifier(self, ctx:TeradataParser.TableIdentifierContext):
        pass

    # Exit a parse tree produced by TeradataParser#tableIdentifier.
    def exitTableIdentifier(self, ctx:TeradataParser.TableIdentifierContext):
        pass


    # Enter a parse tree produced by TeradataParser#namedExpression.
    def enterNamedExpression(self, ctx:TeradataParser.NamedExpressionContext):
        pass

    # Exit a parse tree produced by TeradataParser#namedExpression.
    def exitNamedExpression(self, ctx:TeradataParser.NamedExpressionContext):
        pass


    # Enter a parse tree produced by TeradataParser#namedExpressionSeq.
    def enterNamedExpressionSeq(self, ctx:TeradataParser.NamedExpressionSeqContext):
        pass

    # Exit a parse tree produced by TeradataParser#namedExpressionSeq.
    def exitNamedExpressionSeq(self, ctx:TeradataParser.NamedExpressionSeqContext):
        pass


    # Enter a parse tree produced by TeradataParser#expression.
    def enterExpression(self, ctx:TeradataParser.ExpressionContext):
        pass

    # Exit a parse tree produced by TeradataParser#expression.
    def exitExpression(self, ctx:TeradataParser.ExpressionContext):
        pass


    # Enter a parse tree produced by TeradataParser#logicalNot.
    def enterLogicalNot(self, ctx:TeradataParser.LogicalNotContext):
        pass

    # Exit a parse tree produced by TeradataParser#logicalNot.
    def exitLogicalNot(self, ctx:TeradataParser.LogicalNotContext):
        pass


    # Enter a parse tree produced by TeradataParser#booleanDefault.
    def enterBooleanDefault(self, ctx:TeradataParser.BooleanDefaultContext):
        pass

    # Exit a parse tree produced by TeradataParser#booleanDefault.
    def exitBooleanDefault(self, ctx:TeradataParser.BooleanDefaultContext):
        pass


    # Enter a parse tree produced by TeradataParser#exists.
    def enterExists(self, ctx:TeradataParser.ExistsContext):
        pass

    # Exit a parse tree produced by TeradataParser#exists.
    def exitExists(self, ctx:TeradataParser.ExistsContext):
        pass


    # Enter a parse tree produced by TeradataParser#logicalBinary.
    def enterLogicalBinary(self, ctx:TeradataParser.LogicalBinaryContext):
        pass

    # Exit a parse tree produced by TeradataParser#logicalBinary.
    def exitLogicalBinary(self, ctx:TeradataParser.LogicalBinaryContext):
        pass


    # Enter a parse tree produced by TeradataParser#predicated.
    def enterPredicated(self, ctx:TeradataParser.PredicatedContext):
        pass

    # Exit a parse tree produced by TeradataParser#predicated.
    def exitPredicated(self, ctx:TeradataParser.PredicatedContext):
        pass


    # Enter a parse tree produced by TeradataParser#predicate.
    def enterPredicate(self, ctx:TeradataParser.PredicateContext):
        pass

    # Exit a parse tree produced by TeradataParser#predicate.
    def exitPredicate(self, ctx:TeradataParser.PredicateContext):
        pass


    # Enter a parse tree produced by TeradataParser#valueExpressionDefault.
    def enterValueExpressionDefault(self, ctx:TeradataParser.ValueExpressionDefaultContext):
        pass

    # Exit a parse tree produced by TeradataParser#valueExpressionDefault.
    def exitValueExpressionDefault(self, ctx:TeradataParser.ValueExpressionDefaultContext):
        pass


    # Enter a parse tree produced by TeradataParser#comparison.
    def enterComparison(self, ctx:TeradataParser.ComparisonContext):
        pass

    # Exit a parse tree produced by TeradataParser#comparison.
    def exitComparison(self, ctx:TeradataParser.ComparisonContext):
        pass


    # Enter a parse tree produced by TeradataParser#arithmeticBinary.
    def enterArithmeticBinary(self, ctx:TeradataParser.ArithmeticBinaryContext):
        pass

    # Exit a parse tree produced by TeradataParser#arithmeticBinary.
    def exitArithmeticBinary(self, ctx:TeradataParser.ArithmeticBinaryContext):
        pass


    # Enter a parse tree produced by TeradataParser#arithmeticUnary.
    def enterArithmeticUnary(self, ctx:TeradataParser.ArithmeticUnaryContext):
        pass

    # Exit a parse tree produced by TeradataParser#arithmeticUnary.
    def exitArithmeticUnary(self, ctx:TeradataParser.ArithmeticUnaryContext):
        pass


    # Enter a parse tree produced by TeradataParser#teradataCast.
    def enterTeradataCast(self, ctx:TeradataParser.TeradataCastContext):
        pass

    # Exit a parse tree produced by TeradataParser#teradataCast.
    def exitTeradataCast(self, ctx:TeradataParser.TeradataCastContext):
        pass


    # Enter a parse tree produced by TeradataParser#dereference.
    def enterDereference(self, ctx:TeradataParser.DereferenceContext):
        pass

    # Exit a parse tree produced by TeradataParser#dereference.
    def exitDereference(self, ctx:TeradataParser.DereferenceContext):
        pass


    # Enter a parse tree produced by TeradataParser#positionFunctionCall.
    def enterPositionFunctionCall(self, ctx:TeradataParser.PositionFunctionCallContext):
        pass

    # Exit a parse tree produced by TeradataParser#positionFunctionCall.
    def exitPositionFunctionCall(self, ctx:TeradataParser.PositionFunctionCallContext):
        pass


    # Enter a parse tree produced by TeradataParser#simpleCase.
    def enterSimpleCase(self, ctx:TeradataParser.SimpleCaseContext):
        pass

    # Exit a parse tree produced by TeradataParser#simpleCase.
    def exitSimpleCase(self, ctx:TeradataParser.SimpleCaseContext):
        pass


    # Enter a parse tree produced by TeradataParser#substringCall.
    def enterSubstringCall(self, ctx:TeradataParser.SubstringCallContext):
        pass

    # Exit a parse tree produced by TeradataParser#substringCall.
    def exitSubstringCall(self, ctx:TeradataParser.SubstringCallContext):
        pass


    # Enter a parse tree produced by TeradataParser#columnReference.
    def enterColumnReference(self, ctx:TeradataParser.ColumnReferenceContext):
        pass

    # Exit a parse tree produced by TeradataParser#columnReference.
    def exitColumnReference(self, ctx:TeradataParser.ColumnReferenceContext):
        pass


    # Enter a parse tree produced by TeradataParser#rowConstructor.
    def enterRowConstructor(self, ctx:TeradataParser.RowConstructorContext):
        pass

    # Exit a parse tree produced by TeradataParser#rowConstructor.
    def exitRowConstructor(self, ctx:TeradataParser.RowConstructorContext):
        pass


    # Enter a parse tree produced by TeradataParser#star.
    def enterStar(self, ctx:TeradataParser.StarContext):
        pass

    # Exit a parse tree produced by TeradataParser#star.
    def exitStar(self, ctx:TeradataParser.StarContext):
        pass


    # Enter a parse tree produced by TeradataParser#subscript.
    def enterSubscript(self, ctx:TeradataParser.SubscriptContext):
        pass

    # Exit a parse tree produced by TeradataParser#subscript.
    def exitSubscript(self, ctx:TeradataParser.SubscriptContext):
        pass


    # Enter a parse tree produced by TeradataParser#timeFunctionCall.
    def enterTimeFunctionCall(self, ctx:TeradataParser.TimeFunctionCallContext):
        pass

    # Exit a parse tree produced by TeradataParser#timeFunctionCall.
    def exitTimeFunctionCall(self, ctx:TeradataParser.TimeFunctionCallContext):
        pass


    # Enter a parse tree produced by TeradataParser#subqueryExpression.
    def enterSubqueryExpression(self, ctx:TeradataParser.SubqueryExpressionContext):
        pass

    # Exit a parse tree produced by TeradataParser#subqueryExpression.
    def exitSubqueryExpression(self, ctx:TeradataParser.SubqueryExpressionContext):
        pass


    # Enter a parse tree produced by TeradataParser#cast.
    def enterCast(self, ctx:TeradataParser.CastContext):
        pass

    # Exit a parse tree produced by TeradataParser#cast.
    def exitCast(self, ctx:TeradataParser.CastContext):
        pass


    # Enter a parse tree produced by TeradataParser#constantDefault.
    def enterConstantDefault(self, ctx:TeradataParser.ConstantDefaultContext):
        pass

    # Exit a parse tree produced by TeradataParser#constantDefault.
    def exitConstantDefault(self, ctx:TeradataParser.ConstantDefaultContext):
        pass


    # Enter a parse tree produced by TeradataParser#parenthesizedExpression.
    def enterParenthesizedExpression(self, ctx:TeradataParser.ParenthesizedExpressionContext):
        pass

    # Exit a parse tree produced by TeradataParser#parenthesizedExpression.
    def exitParenthesizedExpression(self, ctx:TeradataParser.ParenthesizedExpressionContext):
        pass


    # Enter a parse tree produced by TeradataParser#functionCall.
    def enterFunctionCall(self, ctx:TeradataParser.FunctionCallContext):
        pass

    # Exit a parse tree produced by TeradataParser#functionCall.
    def exitFunctionCall(self, ctx:TeradataParser.FunctionCallContext):
        pass


    # Enter a parse tree produced by TeradataParser#caseSpecificExpression.
    def enterCaseSpecificExpression(self, ctx:TeradataParser.CaseSpecificExpressionContext):
        pass

    # Exit a parse tree produced by TeradataParser#caseSpecificExpression.
    def exitCaseSpecificExpression(self, ctx:TeradataParser.CaseSpecificExpressionContext):
        pass


    # Enter a parse tree produced by TeradataParser#trimCall.
    def enterTrimCall(self, ctx:TeradataParser.TrimCallContext):
        pass

    # Exit a parse tree produced by TeradataParser#trimCall.
    def exitTrimCall(self, ctx:TeradataParser.TrimCallContext):
        pass


    # Enter a parse tree produced by TeradataParser#searchedCase.
    def enterSearchedCase(self, ctx:TeradataParser.SearchedCaseContext):
        pass

    # Exit a parse tree produced by TeradataParser#searchedCase.
    def exitSearchedCase(self, ctx:TeradataParser.SearchedCaseContext):
        pass


    # Enter a parse tree produced by TeradataParser#columnNameReference.
    def enterColumnNameReference(self, ctx:TeradataParser.ColumnNameReferenceContext):
        pass

    # Exit a parse tree produced by TeradataParser#columnNameReference.
    def exitColumnNameReference(self, ctx:TeradataParser.ColumnNameReferenceContext):
        pass


    # Enter a parse tree produced by TeradataParser#concatenateExpression.
    def enterConcatenateExpression(self, ctx:TeradataParser.ConcatenateExpressionContext):
        pass

    # Exit a parse tree produced by TeradataParser#concatenateExpression.
    def exitConcatenateExpression(self, ctx:TeradataParser.ConcatenateExpressionContext):
        pass


    # Enter a parse tree produced by TeradataParser#dataAttribute.
    def enterDataAttribute(self, ctx:TeradataParser.DataAttributeContext):
        pass

    # Exit a parse tree produced by TeradataParser#dataAttribute.
    def exitDataAttribute(self, ctx:TeradataParser.DataAttributeContext):
        pass


    # Enter a parse tree produced by TeradataParser#dataTypePrecision.
    def enterDataTypePrecision(self, ctx:TeradataParser.DataTypePrecisionContext):
        pass

    # Exit a parse tree produced by TeradataParser#dataTypePrecision.
    def exitDataTypePrecision(self, ctx:TeradataParser.DataTypePrecisionContext):
        pass


    # Enter a parse tree produced by TeradataParser#nullLiteral.
    def enterNullLiteral(self, ctx:TeradataParser.NullLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#nullLiteral.
    def exitNullLiteral(self, ctx:TeradataParser.NullLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#intervalLiteral.
    def enterIntervalLiteral(self, ctx:TeradataParser.IntervalLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#intervalLiteral.
    def exitIntervalLiteral(self, ctx:TeradataParser.IntervalLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#typeConstructor.
    def enterTypeConstructor(self, ctx:TeradataParser.TypeConstructorContext):
        pass

    # Exit a parse tree produced by TeradataParser#typeConstructor.
    def exitTypeConstructor(self, ctx:TeradataParser.TypeConstructorContext):
        pass


    # Enter a parse tree produced by TeradataParser#numericLiteral.
    def enterNumericLiteral(self, ctx:TeradataParser.NumericLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#numericLiteral.
    def exitNumericLiteral(self, ctx:TeradataParser.NumericLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#booleanLiteral.
    def enterBooleanLiteral(self, ctx:TeradataParser.BooleanLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#booleanLiteral.
    def exitBooleanLiteral(self, ctx:TeradataParser.BooleanLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#stringLiteral.
    def enterStringLiteral(self, ctx:TeradataParser.StringLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#stringLiteral.
    def exitStringLiteral(self, ctx:TeradataParser.StringLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#caseSpecific.
    def enterCaseSpecific(self, ctx:TeradataParser.CaseSpecificContext):
        pass

    # Exit a parse tree produced by TeradataParser#caseSpecific.
    def exitCaseSpecific(self, ctx:TeradataParser.CaseSpecificContext):
        pass


    # Enter a parse tree produced by TeradataParser#comparisonOperator.
    def enterComparisonOperator(self, ctx:TeradataParser.ComparisonOperatorContext):
        pass

    # Exit a parse tree produced by TeradataParser#comparisonOperator.
    def exitComparisonOperator(self, ctx:TeradataParser.ComparisonOperatorContext):
        pass


    # Enter a parse tree produced by TeradataParser#arithmeticOperator.
    def enterArithmeticOperator(self, ctx:TeradataParser.ArithmeticOperatorContext):
        pass

    # Exit a parse tree produced by TeradataParser#arithmeticOperator.
    def exitArithmeticOperator(self, ctx:TeradataParser.ArithmeticOperatorContext):
        pass


    # Enter a parse tree produced by TeradataParser#predicateOperator.
    def enterPredicateOperator(self, ctx:TeradataParser.PredicateOperatorContext):
        pass

    # Exit a parse tree produced by TeradataParser#predicateOperator.
    def exitPredicateOperator(self, ctx:TeradataParser.PredicateOperatorContext):
        pass


    # Enter a parse tree produced by TeradataParser#booleanValue.
    def enterBooleanValue(self, ctx:TeradataParser.BooleanValueContext):
        pass

    # Exit a parse tree produced by TeradataParser#booleanValue.
    def exitBooleanValue(self, ctx:TeradataParser.BooleanValueContext):
        pass


    # Enter a parse tree produced by TeradataParser#interval.
    def enterInterval(self, ctx:TeradataParser.IntervalContext):
        pass

    # Exit a parse tree produced by TeradataParser#interval.
    def exitInterval(self, ctx:TeradataParser.IntervalContext):
        pass


    # Enter a parse tree produced by TeradataParser#intervalField.
    def enterIntervalField(self, ctx:TeradataParser.IntervalFieldContext):
        pass

    # Exit a parse tree produced by TeradataParser#intervalField.
    def exitIntervalField(self, ctx:TeradataParser.IntervalFieldContext):
        pass


    # Enter a parse tree produced by TeradataParser#intervalValue.
    def enterIntervalValue(self, ctx:TeradataParser.IntervalValueContext):
        pass

    # Exit a parse tree produced by TeradataParser#intervalValue.
    def exitIntervalValue(self, ctx:TeradataParser.IntervalValueContext):
        pass


    # Enter a parse tree produced by TeradataParser#dataType.
    def enterDataType(self, ctx:TeradataParser.DataTypeContext):
        pass

    # Exit a parse tree produced by TeradataParser#dataType.
    def exitDataType(self, ctx:TeradataParser.DataTypeContext):
        pass


    # Enter a parse tree produced by TeradataParser#intervalDataType.
    def enterIntervalDataType(self, ctx:TeradataParser.IntervalDataTypeContext):
        pass

    # Exit a parse tree produced by TeradataParser#intervalDataType.
    def exitIntervalDataType(self, ctx:TeradataParser.IntervalDataTypeContext):
        pass


    # Enter a parse tree produced by TeradataParser#complexDataType.
    def enterComplexDataType(self, ctx:TeradataParser.ComplexDataTypeContext):
        pass

    # Exit a parse tree produced by TeradataParser#complexDataType.
    def exitComplexDataType(self, ctx:TeradataParser.ComplexDataTypeContext):
        pass


    # Enter a parse tree produced by TeradataParser#colTypeList.
    def enterColTypeList(self, ctx:TeradataParser.ColTypeListContext):
        pass

    # Exit a parse tree produced by TeradataParser#colTypeList.
    def exitColTypeList(self, ctx:TeradataParser.ColTypeListContext):
        pass


    # Enter a parse tree produced by TeradataParser#colType.
    def enterColType(self, ctx:TeradataParser.ColTypeContext):
        pass

    # Exit a parse tree produced by TeradataParser#colType.
    def exitColType(self, ctx:TeradataParser.ColTypeContext):
        pass


    # Enter a parse tree produced by TeradataParser#whenClause.
    def enterWhenClause(self, ctx:TeradataParser.WhenClauseContext):
        pass

    # Exit a parse tree produced by TeradataParser#whenClause.
    def exitWhenClause(self, ctx:TeradataParser.WhenClauseContext):
        pass


    # Enter a parse tree produced by TeradataParser#windows.
    def enterWindows(self, ctx:TeradataParser.WindowsContext):
        pass

    # Exit a parse tree produced by TeradataParser#windows.
    def exitWindows(self, ctx:TeradataParser.WindowsContext):
        pass


    # Enter a parse tree produced by TeradataParser#namedWindow.
    def enterNamedWindow(self, ctx:TeradataParser.NamedWindowContext):
        pass

    # Exit a parse tree produced by TeradataParser#namedWindow.
    def exitNamedWindow(self, ctx:TeradataParser.NamedWindowContext):
        pass


    # Enter a parse tree produced by TeradataParser#windowRef.
    def enterWindowRef(self, ctx:TeradataParser.WindowRefContext):
        pass

    # Exit a parse tree produced by TeradataParser#windowRef.
    def exitWindowRef(self, ctx:TeradataParser.WindowRefContext):
        pass


    # Enter a parse tree produced by TeradataParser#windowDef.
    def enterWindowDef(self, ctx:TeradataParser.WindowDefContext):
        pass

    # Exit a parse tree produced by TeradataParser#windowDef.
    def exitWindowDef(self, ctx:TeradataParser.WindowDefContext):
        pass


    # Enter a parse tree produced by TeradataParser#windowFrame.
    def enterWindowFrame(self, ctx:TeradataParser.WindowFrameContext):
        pass

    # Exit a parse tree produced by TeradataParser#windowFrame.
    def exitWindowFrame(self, ctx:TeradataParser.WindowFrameContext):
        pass


    # Enter a parse tree produced by TeradataParser#frameBound.
    def enterFrameBound(self, ctx:TeradataParser.FrameBoundContext):
        pass

    # Exit a parse tree produced by TeradataParser#frameBound.
    def exitFrameBound(self, ctx:TeradataParser.FrameBoundContext):
        pass


    # Enter a parse tree produced by TeradataParser#qualifiedName.
    def enterQualifiedName(self, ctx:TeradataParser.QualifiedNameContext):
        pass

    # Exit a parse tree produced by TeradataParser#qualifiedName.
    def exitQualifiedName(self, ctx:TeradataParser.QualifiedNameContext):
        pass


    # Enter a parse tree produced by TeradataParser#identifier.
    def enterIdentifier(self, ctx:TeradataParser.IdentifierContext):
        pass

    # Exit a parse tree produced by TeradataParser#identifier.
    def exitIdentifier(self, ctx:TeradataParser.IdentifierContext):
        pass


    # Enter a parse tree produced by TeradataParser#strictIdentifier.
    def enterStrictIdentifier(self, ctx:TeradataParser.StrictIdentifierContext):
        pass

    # Exit a parse tree produced by TeradataParser#strictIdentifier.
    def exitStrictIdentifier(self, ctx:TeradataParser.StrictIdentifierContext):
        pass


    # Enter a parse tree produced by TeradataParser#quotedIdentifier.
    def enterQuotedIdentifier(self, ctx:TeradataParser.QuotedIdentifierContext):
        pass

    # Exit a parse tree produced by TeradataParser#quotedIdentifier.
    def exitQuotedIdentifier(self, ctx:TeradataParser.QuotedIdentifierContext):
        pass


    # Enter a parse tree produced by TeradataParser#decimalLiteral.
    def enterDecimalLiteral(self, ctx:TeradataParser.DecimalLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#decimalLiteral.
    def exitDecimalLiteral(self, ctx:TeradataParser.DecimalLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#scientificDecimalLiteral.
    def enterScientificDecimalLiteral(self, ctx:TeradataParser.ScientificDecimalLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#scientificDecimalLiteral.
    def exitScientificDecimalLiteral(self, ctx:TeradataParser.ScientificDecimalLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#integerLiteral.
    def enterIntegerLiteral(self, ctx:TeradataParser.IntegerLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#integerLiteral.
    def exitIntegerLiteral(self, ctx:TeradataParser.IntegerLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#bigIntLiteral.
    def enterBigIntLiteral(self, ctx:TeradataParser.BigIntLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#bigIntLiteral.
    def exitBigIntLiteral(self, ctx:TeradataParser.BigIntLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#smallIntLiteral.
    def enterSmallIntLiteral(self, ctx:TeradataParser.SmallIntLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#smallIntLiteral.
    def exitSmallIntLiteral(self, ctx:TeradataParser.SmallIntLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#tinyIntLiteral.
    def enterTinyIntLiteral(self, ctx:TeradataParser.TinyIntLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#tinyIntLiteral.
    def exitTinyIntLiteral(self, ctx:TeradataParser.TinyIntLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#doubleLiteral.
    def enterDoubleLiteral(self, ctx:TeradataParser.DoubleLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#doubleLiteral.
    def exitDoubleLiteral(self, ctx:TeradataParser.DoubleLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#bigDecimalLiteral.
    def enterBigDecimalLiteral(self, ctx:TeradataParser.BigDecimalLiteralContext):
        pass

    # Exit a parse tree produced by TeradataParser#bigDecimalLiteral.
    def exitBigDecimalLiteral(self, ctx:TeradataParser.BigDecimalLiteralContext):
        pass


    # Enter a parse tree produced by TeradataParser#nonReserved.
    def enterNonReserved(self, ctx:TeradataParser.NonReservedContext):
        pass

    # Exit a parse tree produced by TeradataParser#nonReserved.
    def exitNonReserved(self, ctx:TeradataParser.NonReservedContext):
        pass



del TeradataParser