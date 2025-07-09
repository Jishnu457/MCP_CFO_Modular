"""
Prompt management and intent detection
"""
import json
import re
from typing import List, Dict
import structlog
from utils.helpers import Utils

logger = structlog.get_logger()

class PromptManager:
    """Centralized prompt and intent management with enhanced GROUP BY rules"""
    
    def __init__(self, ai_services):
        self.ai_services = ai_services
    
    def load_base_prompt(self):
        """Enhanced base prompt with comprehensive few-shot learning"""
        
        few_shot_examples = """
------ EXAMPLE 1: Revenue Analysis with Proper Client Filtering
User: Show me revenue for Brown Ltd in 2024 and 2025
System: You need to filter by client and use DATEPART for year extraction from date columns:

SELECT 
    [Client],
    DATEPART(YEAR, [Date]) AS [Year],
    SUM([Revenue]) AS [Total_Revenue]
FROM [dbo].[Financial] 
WHERE [Client] = 'Brown Ltd' 
    AND DATEPART(YEAR, [Date]) IN (2024, 2025)
GROUP BY [Client], DATEPART(YEAR, [Date])
ORDER BY [Year];

------ EXAMPLE 2: Contextual Follow-up Analysis with Context Preservation
User: Why does the revenue behave this way?
System: To analyze revenue behavior, calculate growth rates and trends using the same client filter from previous query:

SELECT 
    [Client],
    DATEPART(YEAR, [Date]) AS [Year],
    DATEPART(QUARTER, [Date]) AS [Quarter],
    SUM([Revenue]) AS Quarterly_Revenue,
    LAG(SUM([Revenue])) OVER (
        PARTITION BY [Client] 
        ORDER BY DATEPART(YEAR, [Date]), DATEPART(QUARTER, [Date])
    ) AS Previous_Quarter,
    CASE 
        WHEN LAG(SUM([Revenue])) OVER (
            PARTITION BY [Client] 
            ORDER BY DATEPART(YEAR, [Date]), DATEPART(QUARTER, [Date])
        ) > 0 
        THEN ((SUM([Revenue]) - LAG(SUM([Revenue])) OVER (
            PARTITION BY [Client] 
            ORDER BY DATEPART(YEAR, [Date]), DATEPART(QUARTER, [Date])
        )) / LAG(SUM([Revenue])) OVER (
            PARTITION BY [Client] 
            ORDER BY DATEPART(YEAR, [Date]), DATEPART(QUARTER, [Date])
        )) * 100 
        ELSE NULL 
    END AS Growth_Rate_Percent
FROM [dbo].[Financial] 
WHERE [Client] = 'Brown Ltd' 
    AND DATEPART(YEAR, [Date]) IN (2024, 2025)
GROUP BY [Client], DATEPART(YEAR, [Date]), DATEPART(QUARTER, [Date])
ORDER BY [Year], [Quarter];

------ EXAMPLE 3: Profit & Loss Report Structure
User: Create a P&L report for 2025
System: For comprehensive P&L analysis, aggregate multiple financial metrics by time period:

SELECT 
    DATEPART(QUARTER, [Date]) AS [Quarter],
    DATEPART(MONTH, [Date]) AS [Month],
    SUM([Revenue]) AS Total_Revenue,
    SUM([Gross Profit]) AS Total_Gross_Profit,
    SUM([Net Income]) AS Total_Net_Income,
    (SUM([Gross Profit]) / NULLIF(SUM([Revenue]), 0)) * 100 AS Gross_Margin_Percent,
    (SUM([Net Income]) / NULLIF(SUM([Revenue]), 0)) * 100 AS Net_Margin_Percent
FROM [dbo].[Financial] 
WHERE DATEPART(YEAR, [Date]) = 2025
GROUP BY DATEPART(QUARTER, [Date]), DATEPART(MONTH, [Date])
ORDER BY [Quarter], [Month];
"""
        
        return f"""You are an expert SQL analyst specializing in financial data analysis. You must respond in this EXACT format:

SQL_QUERY:
[Complete SQL statement using exact column names from schema]

ANALYSIS:
[Brief explanation of results and insights]

🎯 CRITICAL SUCCESS FACTORS:

1. **Schema Adherence**: Use ONLY columns that exist in the provided schema
2. **Context Preservation**: For follow-up questions, maintain client/entity filters from previous queries
3. **Professional Formatting**: Use clean, readable SQL formatting with proper indentation
4. **Date Handling**: Use DATEPART(YEAR/MONTH/QUARTER, [Date]) for date-based grouping
5. **Analytical Depth**: For "why" questions, include growth rates, trends, and comparisons

📚 LEARN FROM THESE EXAMPLES:
{few_shot_examples}

✅ PROVEN PATTERNS:
- Client Analysis: WHERE [Client] = 'ClientName' AND DATEPART(YEAR, [Date]) IN (2024, 2025)
- Growth Calculation: LAG() OVER (PARTITION BY [Client] ORDER BY [Year], [Quarter])  
- Trend Analysis: Use DATEPART for time-based grouping
- Context Questions: Preserve WHERE conditions from previous successful queries
- P&L Reports: Aggregate Revenue, Gross Profit, Net Income with margin calculations

❌ AVOID THESE COMMON MISTAKES:
- Missing spaces: GROUP BY[column] → should be GROUP BY [column]
- Wrong columns: Use exact names from schema, not assumptions
- Lost context: Follow-up questions must preserve client/entity filters
- Incomplete GROUP BY: All non-aggregate SELECT columns must be in GROUP BY
- Date errors: Use DATEPART() for date extraction, handle NULLs

🔧 SQL SYNTAX RULES:
- Always space after keywords: "GROUP BY [column]" not "GROUP BY[column]"
- Use proper WHERE clause combining: WHERE condition1 AND condition2
- Handle division by zero: NULLIF(denominator, 0) in calculations
- Order results logically: ORDER BY [Year], [Quarter], [Month]

REMEMBER: Your goal is to generate SQL that a financial analyst can immediately execute to get business insights!
"""
    
    def format_schema_for_prompt(self, tables_info: List[Dict]) -> str:
        return f"AVAILABLE SCHEMA:\n{json.dumps(tables_info, indent=2, default=Utils.safe_json_serialize)}"
    
    def filter_schema_for_question(self, question: str, tables_info: List[Dict]) -> List[Dict]:
        question_lower = question.lower()
        
        # For P&L/financial questions, force Financial table to the top
        if any(word in question_lower for word in ['p&l', 'profit', 'loss', 'financial', 'revenue']):
            result = []
            financial_table = None
            other_financial = []
            remaining = []
            
            for table in tables_info:
                table_name = table.get('table', '').lower()
                
                # Find Financial table first
                if 'financial' in table_name:
                    financial_table = table
                elif any(term in table_name for term in ['sales', 'revenue', 'balance', 'income']):
                    other_financial.append(table)
                else:
                    remaining.append(table)
            
            # Put Financial table first, then other financial tables
            if financial_table:
                result.append(financial_table)
            result.extend(other_financial[:2])  # Max 2 other financial tables
            result.extend(remaining[:2])       # Max 2 other tables
            
            return result
        
        # For other questions, use existing logic
        question_terms = set(term for term in question_lower.split() if len(term) > 2)
        relevant_tables = []
        
        for table_info in tables_info:
            table_name = table_info['table'].lower()
            table_base_name = table_name.split('.')[-1].strip('[]')
            columns = [col.lower() for col in table_info.get('columns', [])]
            table_terms = set([table_base_name] + [col.split()[0] for col in columns])
            
            if question_terms.intersection(table_terms):
                relevant_tables.append(table_info)
        
        return relevant_tables or tables_info
    
    async def build_chatgpt_system_prompt(self, question: str, tables_info: List[Dict], conversation_history: List[Dict] = None) -> str:
        """Simplified prompt building without complex context logic"""
        
        base_prompt = self.load_base_prompt()
        schema_section = self.format_schema_for_prompt(self.filter_schema_for_question(question, tables_info))
        
        # Simplified question analysis without complex context
        question_analysis = f"""
🎯 CURRENT REQUEST ANALYSIS:
User Question: "{question}"

INSTRUCTIONS:
1. **Schema Validation**: Use ONLY the tables and columns shown below in the schema
2. **Professional Output**: Format SQL with proper spacing and readable structure
3. **Business Focus**: Provide SQL that delivers actionable business insights

🆕 NEW QUERY PROCESSING: Comprehensive analysis of the dataset.
"""
        
        return f"{base_prompt}\n\n{schema_section}\n\n{question_analysis}"
    
    def extract_filters_from_sql(self, sql: str) -> List[str]:
        """Extract WHERE conditions from previous SQL to preserve context"""
        
        if not sql:
            return []
        
        try:
            sql_upper = sql.upper()
            
            # Find WHERE clause
            where_start = sql_upper.find(' WHERE ')
            if where_start == -1:
                return []
            
            # Find end of WHERE clause (before GROUP BY, ORDER BY, etc.)
            where_end = len(sql)
            for keyword in [' GROUP BY', ' ORDER BY', ' HAVING']:
                pos = sql_upper.find(keyword, where_start)
                if pos != -1:
                    where_end = min(where_end, pos)
            
            where_clause = sql[where_start + 7:where_end].strip()
            
            # Split by AND/OR and clean up
            conditions = []
            for condition in where_clause.split(' AND '):
                condition = condition.strip()
                if condition and not condition.upper().startswith('OR'):
                    # Clean up the condition
                    if condition.startswith('(') and condition.endswith(')'):
                        condition = condition[1:-1]
                    conditions.append(condition)
            
            logger.info("Extracted SQL filters", original_sql=sql, filters=conditions)
            return conditions
            
        except Exception as e:
            logger.warning("Failed to extract filters from SQL", error=str(e), sql=sql)
            return []