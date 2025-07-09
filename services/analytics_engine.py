"""
Main analytics engine - handles question processing and SQL generation
"""
import asyncio
import json
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
import structlog

from utils.helpers import Utils

logger = structlog.get_logger()

current_year = datetime.now().year

class AnalyticsEngine:
    """Main analytics engine - consolidated logic"""
    
    def __init__(self, db_manager, schema_manager, kql_storage, ai_services, viz_manager, prompt_manager):
        self.db_manager = db_manager
        self.schema_manager = schema_manager
        self.kql_storage = kql_storage
        self.ai_services = ai_services
        self.viz_manager = viz_manager
        self.prompt_manager = prompt_manager
    
    '''def is_contextual_question(self, question: str) -> bool:
        """Detect if a question refers to previous context"""
        question_lower = question.lower().strip()
        
        # Strong contextual indicators
        strong_contextual = [
            'why does it', 'why does this', 'why does that',
            'how does it', 'how does this', 'how does that', 
            'what does it', 'what does this', 'what does that',
            'how do i improve this', 'how can i improve this', 'how to improve this',
            'explain this', 'explain it', 'analyze this', 'analyze it',
            'what causes this', 'what causes it', 'what makes this', 'what makes it',
            'this behavior', 'this pattern', 'this trend', 'this way',
            'why is this', 'why is it', 'how is this', 'how is it'
        ]
        
        # Check for strong indicators first
        for indicator in strong_contextual:
            if indicator in question_lower:
                return True
        
        # Weak contextual indicators (only contextual in specific contexts)
        weak_contextual = ['this', 'it', 'that', 'them', 'their']
        
        # Only consider weak indicators if question is very short and contains improvement words
        if len(question_lower.split()) <= 4:
            has_weak_pronoun = any(pronoun in question_lower for pronoun in weak_contextual)
            has_improvement_words = any(word in question_lower for word in ['improve', 'increase', 'fix', 'change', 'why', 'how', 'explain'])
            has_data_words = any(word in question_lower for word in ['show', 'get', 'find', 'list', 'display', 'revenue', 'profit', 'sales'])
            
            if has_weak_pronoun and has_improvement_words and not has_data_words:
                return True
        
        return False'''
    
    def is_contextual_question(self, question: str) -> bool:
        """Simplified contextual detection"""
        question_lower = question.lower().strip()
        
        # Only very obvious contextual patterns
        obvious_contextual = ['why', 'how', 'what', 'explain', 'analyze']
        
        # If it has contextual words AND is short, it's probably contextual
        if len(question_lower.split()) <= 4:
            return any(word in question_lower for word in obvious_contextual)
        
    
    def is_data_question(self, question: str) -> bool:
        """Let the model decide if this needs data - minimal classification"""
        question_lower = question.lower().strip()
        
        # Only filter out obvious non-data questions
        obvious_non_data = [
            'hello', 'hi', 'hey', 'thanks', 'thank you', 
            'what can you do', 'help', 'how are you'
        ]
        
        return question_lower not in obvious_non_data
    
    '''def is_data_question(self, question: str) -> bool:
        """Detect if this is a data analysis question that should generate SQL"""
        question_lower = question.lower().strip()
        
        # Data analysis keywords
        data_keywords = [
            'show', 'get', 'find', 'list', 'display', 'what is', 'what are',
            'how much', 'how many', 'count', 'sum', 'total', 'average', 'avg',
            'max', 'min', 'calculate', 'analysis', 'analyze', 'report',
            'revenue', 'profit', 'margin', 'sales', 'cost', 'expense',
            'gross', 'net', 'income', 'client', 'customer', 'year', 'quarter',
            'month', 'trend', 'growth', 'performance', 'breakdown', 'compare', 
            'comparison', 'profitability', 'driving', 'specific',
            'products', 'services', 'business unit', 'business units',
            'decline', 'increase', 'higher', 'lower', 'best', 'worst',
            'top', 'bottom', 'rank', 'ranking', 'most', 'least'
        ]
        
        # Time indicators
        time_keywords = [
            '2020', '2021', '2022', '2023', '2024', '2025',
            'quarter', 'q1', 'q2', 'q3', 'q4', 'monthly', 'yearly'
        ]
        
        # Metric keywords
        metric_keywords = [
            'margin', 'profit', 'revenue', 'income', 'cost', 'expense',
            'ratio', 'percentage', 'rate', 'amount', 'value'
        ]
        
        has_data_keywords = any(keyword in question_lower for keyword in data_keywords)
        has_time_keywords = any(keyword in question_lower for keyword in time_keywords)
        has_metric_keywords = any(keyword in question_lower for keyword in metric_keywords)
        
        return has_data_keywords or has_metric_keywords or has_time_keywords'''
    
    # Add this parameter to cached_intelligent_analyze:

    async def cached_intelligent_analyze(self, question: str, session_id: str = None, enable_ai_insights: bool = False, return_raw_data: bool = False) -> Dict[str, Any]:
        """Main entry point with caching support and natural response formatting"""
        actual_session_id = session_id if session_id else "default-session-1234567890"
        
        # Skip KQL cache lookup for schema queries
        if question.lower() in ['tables_info', 'schema_info']:
            raw_result = await self.intelligent_analyze_with_context(question, actual_session_id, enable_ai_insights, None)
            if return_raw_data:
                return raw_result  # Return raw data for reports
            return await self._format_natural_response(question, raw_result)
        
        is_contextual = self.is_contextual_question(question)
        
        # For contextual questions, always process fresh (don't use cache)
        if is_contextual:
            logger.info("Contextual question detected, processing fresh", 
                    question=question, session_id=actual_session_id)
            
            raw_result = await self.intelligent_analyze_with_context(question, actual_session_id, enable_ai_insights, None)
            
            try:
                await self.kql_storage.store_in_kql(question, raw_result, [], actual_session_id)
            except Exception as e:
                logger.error("KQL storage failed for contextual question", error=str(e))
            
            # 🔥 NEW: Return raw data for reports, formatted for chat
            if return_raw_data:
                return raw_result
            return await self._format_natural_response(question, raw_result)
        
        # Check KQL cache for non-contextual questions
        cached_result = await self.kql_storage.get_from_kql_cache(question, actual_session_id)
        
        if cached_result:
            logger.info("Cache hit for non-contextual question", question=question, session_id=actual_session_id)
            cached_result["session_id"] = actual_session_id
            # 🔥 NEW: Return raw cached data for reports
            if return_raw_data:
                return cached_result
            return await self._format_natural_response(question, cached_result)
        
        # Process new question
        raw_result = await self.intelligent_analyze_with_context(question, actual_session_id, enable_ai_insights, None)
        
        try:
            await self.kql_storage.store_in_kql(question, raw_result, [], actual_session_id)
            await asyncio.sleep(0.1)  # Small delay for KQL consistency
            logger.info("Processed and stored result", question=question, session_id=actual_session_id)
        except Exception as e:
            logger.error("KQL storage failed", error=str(e))
            # Don't fail the entire request if storage fails
        
        # Return raw data for reports, formatted for chat
        if return_raw_data:
            return raw_result
        return await self._format_natural_response(question, raw_result)
    
    async def _format_natural_response(self, question: str, raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """Format the response naturally like Claude/ChatGPT"""
        try:
            # Import here to avoid circular dependency
            from services.response_formatter import ResponseFormatter, SmartResponseEnhancer
            
            formatter = ResponseFormatter(self.ai_services)
            enhancer = SmartResponseEnhancer(self.ai_services)
            
            # Format the response naturally
            formatted_response = await formatter.format_response(question, raw_result)
            
            # Add contextual enhancements
            enhanced_response = await enhancer.enhance_with_context(formatted_response, question, raw_result)
            
            return enhanced_response
            
        except Exception as e:
            logger.error("Natural response formatting failed", error=str(e))
            # Fallback to simple natural format
            return self._simple_natural_fallback(question, raw_result)
    
    def _simple_natural_fallback(self, question: str, raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """Simple fallback for natural responses"""
        
        # Determine message based on result type
        if "error" in raw_result:
            message = f"I had some trouble with that question. {raw_result.get('suggestion', 'Could you try rephrasing it?')}"
            response_type = "help"
        elif raw_result.get("response_type") == "conversational":
            message = raw_result.get("analysis", "I'm here to help with your data analysis needs.")
            response_type = "conversational"
        elif raw_result.get("result_count", 0) > 0:
            count = raw_result["result_count"]
            analysis = raw_result.get("analysis", "")
            message = f"I found {count} records for your question about {question.lower()}. {analysis}"
            response_type = "analysis"
        else:
            message = raw_result.get("analysis", f"I understand you're asking about {question.lower()}. Let me help you with that.")
            response_type = "response"
        
        response = {
            "message": message,
            "type": response_type,
            "session_id": raw_result.get("session_id"),
            "timestamp": datetime.now().isoformat()
        }
        
        # Add data if available and relevant
        if raw_result.get("result_count", 0) > 0:
            response["found_records"] = raw_result["result_count"]
            if raw_result["result_count"] <= 10:
                response["data"] = raw_result.get("sample_data", [])
            else:
                response["data_sample"] = raw_result.get("sample_data", [])[:5]
        
        # Add visualization if available
        if raw_result.get("visualization"):
            response["chart"] = raw_result["visualization"]
        
        return response
    
    '''async def intelligent_analyze_with_context(self, question: str, session_id: str = None, 
                                             enable_ai_insights: bool = False, 
                                             conversation_history: List[Dict] = None) -> Dict[str, Any]:
        """Main analysis logic with context handling"""
        start_total = time.time()
        actual_session_id = session_id if session_id else "default-session-1234567890"
        
        try:
            logger.info("Starting context analysis", 
                       question=question, 
                       session_id=actual_session_id)
            
            # Handle casual greetings
            if self.is_casual_greeting(question):
                return await self.handle_casual_greeting(question, actual_session_id, conversation_history or [], enable_ai_insights)
            
            # Get conversation history if not provided
            if conversation_history is None:
                conversation_history = await self.get_simple_conversation_history(actual_session_id)
            
            # Get tables info
            tables_info = await self.schema_manager.get_cached_tables_info()
            if not tables_info:
                return self.create_error_response("No accessible tables found.", 
                                                "The system couldn't find any tables in your database.",
                                                "Check database connection and permissions.",
                                                actual_session_id, conversation_history or [], enable_ai_insights)
            
            is_data_question = self.is_data_question(question)
            is_contextual = self.is_contextual_question(question)
            
            if is_contextual and conversation_history:
                # Check for previous data in conversation history
                for msg in conversation_history:
                    if msg.get('role') == 'assistant' and 'SQL Query:' in msg.get('content', ''):
                        is_data_question = True
                        break
            
            if not is_data_question:
                # Handle as conversational
                return await self.handle_conversational_question(question, actual_session_id, conversation_history or [], enable_ai_insights)
            
            # Build prompt with conversation context
            enhanced_prompt = await self.build_prompt_with_conversation(question, tables_info, conversation_history)
            
            try:
                llm_response = await self.ai_services.ask_intelligent_llm_async(enhanced_prompt)
                
                # Extract SQL and analysis
                generated_sql = ""
                analysis = ""
                
                if "SQL_QUERY:" in llm_response and "ANALYSIS:" in llm_response:
                    parts = llm_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
                    generated_sql = Utils.clean_generated_sql(parts[0].strip())
                    analysis = parts[1].strip()
                elif "SQL_QUERY:" in llm_response:
                    parts = llm_response.split("SQL_QUERY:", 1)
                    generated_sql = Utils.clean_generated_sql(parts[1].strip())
                    analysis = "SQL query generated"
                elif "SELECT" in llm_response.upper():
                    # Extract SELECT statements
                    lines = llm_response.split('\n')
                    sql_lines = []
                    in_sql_block = False
                    
                    for line in lines:
                        line_upper = line.strip().upper()
                        if line_upper.startswith('SELECT'):
                            in_sql_block = True
                            sql_lines = [line]
                        elif in_sql_block:
                            if line.strip() and not line.strip().startswith('--'):
                                if any(keyword in line_upper for keyword in ['FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'JOIN', 'AND', 'OR']):
                                    sql_lines.append(line)
                                elif line.strip().endswith(';'):
                                    sql_lines.append(line)
                                    break
                                elif not line.strip():
                                    break
                            elif not line.strip():
                                break
                    
                    if sql_lines:
                        generated_sql = Utils.clean_generated_sql(' '.join(sql_lines))
                        sql_end_idx = llm_response.find(sql_lines[-1]) + len(sql_lines[-1])
                        remaining_text = llm_response[sql_end_idx:].strip()
                        analysis = remaining_text[:500] if remaining_text else "SQL extracted from response"
                
                # Execute SQL if we have it
                if generated_sql and generated_sql.upper().startswith("SELECT"):
                    try:
                        is_valid, validation_message = self.validate_sql_against_schema(generated_sql, tables_info)
        
                        if not is_valid:
                            logger.error("SQL validation failed", 
                                        sql=generated_sql, 
                                        validation_error=validation_message)
                            
                            return {
                                "question": question,
                                "response_type": "error",
                                "error": f"Invalid SQL generated: {validation_message}",
                                "analysis": f"I generated SQL that references tables that don't exist in your database. {validation_message}",
                                "suggestion": f"Let me help you with the available tables. Your database contains: {', '.join([t.get('table', '') for t in tables_info[:3]])}...",
                                "generated_sql": generated_sql,  # Show the problematic SQL for debugging
                                "timestamp": datetime.now().isoformat(),
                                "session_id": actual_session_id,
                                "ai_insights_enabled": enable_ai_insights
                            }
                        
                        # If validation passes, execute the SQL
                        results = await self.execute_sql_query(generated_sql)
                        logger.info("SQL execution successful", result_count=len(results))
                        
                    except Exception as sql_error:
                        logger.error("SQL execution failed", error=str(sql_error))
                        return self.create_error_response(f"SQL execution error: {str(sql_error)}", 
                                                        "The generated SQL query failed to execute.",
                                                        "There may be an issue with the query syntax or database schema.",
                                                        actual_session_id, [], enable_ai_insights)
                    
                    # Build response
                    response = {
                        "question": question,
                        "generated_sql": generated_sql,
                        "analysis": analysis,
                        "result_count": len(results),
                        "sample_data": results[:5] if results else [],
                        "timestamp": datetime.now().isoformat(),
                        "session_id": actual_session_id,
                        "ai_insights_enabled": enable_ai_insights,
                        "conversation_context_used": len(conversation_history) > 0
                    }
                    
                    # Add visualization if appropriate
                    await self.viz_manager.add_visualization_to_response(question, generated_sql, results, response)
                    
                    # Enhanced analysis
                    if results and enable_ai_insights:
                        await self.add_enhanced_analysis(question, generated_sql, results, {}, response, enable_ai_insights)
                    
                    logger.info("Analysis completed", 
                               duration=time.time() - start_total,
                               result_count=len(results))
                    
                    return response
                else:
                    # No valid SQL generated
                    return {
                        "question": question,
                        "response_type": "error",
                        "analysis": f"I couldn't generate SQL for your question: '{question}'. This appears to be a data question but I wasn't able to create a valid query.",
                        "suggestion": "Try rephrasing your question more specifically, such as 'Show me gross margin by year' or 'Calculate gross margin for 2024 and 2025'",
                        "timestamp": datetime.now().isoformat(),
                        "session_id": actual_session_id,
                        "ai_insights_enabled": enable_ai_insights
                    }
            
            except Exception as e:
                logger.error("LLM processing failed", error=str(e))
                return self.create_error_response(f"LLM error: {str(e)}", 
                                                "Failed to process your question with the AI model.",
                                                "Try rephrasing your question more clearly.",
                                                actual_session_id, [], enable_ai_insights)
                
        except Exception as e:
            logger.error("Analysis failed", error=str(e))
            return self.create_error_response(f"Analysis error: {str(e)}",
                                            "I encountered an error while analyzing your question.",
                                            "Try rephrasing your question with more specific details.",
                                            actual_session_id, [], enable_ai_insights)'''
                                            
    async def intelligent_analyze_with_context(self, question: str, session_id: str = None, 
                                     enable_ai_insights: bool = False, 
                                     conversation_history: List[Dict] = None) -> Dict[str, Any]:
        """Simplified approach - let the model handle everything"""
        start_total = time.time()
        actual_session_id = session_id if session_id else "default-session-1234567890"
        
        try:
            logger.info("Starting analysis", question=question, session_id=actual_session_id)
            
            # Get conversation history if not provided
            if conversation_history is None:
                conversation_history = await self.get_simple_conversation_history(actual_session_id)
            
            # Get tables info
            tables_info = await self.schema_manager.get_cached_tables_info()
            if not tables_info:
                return self.create_error_response("No accessible tables found.", 
                                                "The system couldn't find any tables in your database.",
                                                "Check database connection and permissions.",
                                                actual_session_id, conversation_history or [], enable_ai_insights)
            
            # Build prompt and let model decide what to do
            enhanced_prompt = await self.build_prompt_with_conversation(question, tables_info, conversation_history)
            
            # Add instruction for the model to decide
            enhanced_prompt += """
            
    🤖 INSTRUCTIONS:
    If this question can be answered with data from the database, generate SQL.
    If this is a casual greeting or general question, provide a conversational response.
    If you're unsure, lean towards generating SQL - it's better to try and fail than to not try at all.

    RESPONSE FORMAT:
    SQL_QUERY:
    [SQL query OR write "NO_SQL_NEEDED" if this is purely conversational]

    ANALYSIS:
    [Your analysis or conversational response]
    """
            
            try:
                llm_response = await self.ai_services.ask_intelligent_llm_async(enhanced_prompt)
                
                # Check if model decided to generate SQL
                if "SQL_QUERY:" in llm_response and "NO_SQL_NEEDED" not in llm_response:
                    # Extract SQL and analysis
                    generated_sql = ""
                    analysis = ""
                    
                    if "SQL_QUERY:" in llm_response and "ANALYSIS:" in llm_response:
                        parts = llm_response.split("SQL_QUERY:", 1)[1].split("ANALYSIS:", 1)
                        generated_sql = Utils.clean_generated_sql(parts[0].strip())
                        analysis = parts[1].strip()
                    elif "SQL_QUERY:" in llm_response:
                        parts = llm_response.split("SQL_QUERY:", 1)
                        generated_sql = Utils.clean_generated_sql(parts[1].strip())
                        analysis = "SQL query generated"
                    elif "SELECT" in llm_response.upper():
                        # Extract SELECT statements
                        lines = llm_response.split('\n')
                        sql_lines = []
                        in_sql_block = False
                        
                        for line in lines:
                            line_upper = line.strip().upper()
                            if line_upper.startswith('SELECT'):
                                in_sql_block = True
                                sql_lines = [line]
                            elif in_sql_block:
                                if line.strip() and not line.strip().startswith('--'):
                                    if any(keyword in line_upper for keyword in ['FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'JOIN', 'AND', 'OR']):
                                        sql_lines.append(line)
                                    elif line.strip().endswith(';'):
                                        sql_lines.append(line)
                                        break
                                    elif not line.strip():
                                        break
                                elif not line.strip():
                                    break
                        
                        if sql_lines:
                            generated_sql = Utils.clean_generated_sql(' '.join(sql_lines))
                            sql_end_idx = llm_response.find(sql_lines[-1]) + len(sql_lines[-1])
                            remaining_text = llm_response[sql_end_idx:].strip()
                            analysis = remaining_text[:500] if remaining_text else "SQL extracted from response"
                    
                    # Execute SQL if we have it
                    if generated_sql and generated_sql.upper().startswith("SELECT"):
                        try:
                            # Optional: Simple validation (you can remove this if you want)
                            is_valid, validation_message = self.validate_sql_against_schema(generated_sql, tables_info)
                            if not is_valid:
                                logger.error("SQL validation failed", 
                                            sql=generated_sql, 
                                            validation_error=validation_message)
                                return {
                                    "question": question,
                                    "response_type": "error",
                                    "error": f"Invalid SQL generated: {validation_message}",
                                    "analysis": f"I generated SQL that references tables that don't exist in your database. {validation_message}",
                                    "suggestion": f"Let me help you with the available tables. Your database contains: {', '.join([t.get('table', '') for t in tables_info[:3]])}...",
                                    "generated_sql": generated_sql,
                                    "timestamp": datetime.now().isoformat(),
                                    "session_id": actual_session_id,
                                    "ai_insights_enabled": enable_ai_insights
                                }
                            
                            # Execute SQL
                            results = await self.execute_sql_query(generated_sql)
                            logger.info("SQL execution successful", result_count=len(results))
                            
                            # Build response
                            response = {
                                "question": question,
                                "generated_sql": generated_sql,
                                "analysis": analysis,
                                "result_count": len(results),
                                "sample_data": results[:5] if results else [],
                                "timestamp": datetime.now().isoformat(),
                                "session_id": actual_session_id,
                                "ai_insights_enabled": enable_ai_insights,
                                "conversation_context_used": len(conversation_history) > 0
                            }
                            
                            # Add visualization if appropriate
                            await self.viz_manager.add_visualization_to_response(question, generated_sql, results, response)
                            
                            # Enhanced analysis
                            if results and enable_ai_insights:
                                await self.add_enhanced_analysis(question, generated_sql, results, {}, response, enable_ai_insights)
                            
                            logger.info("Analysis completed", 
                                    duration=time.time() - start_total,
                                    result_count=len(results))
                            
                            return response
                            
                        except Exception as sql_error:
                            logger.error("SQL execution failed", error=str(sql_error))
                            return self.create_error_response(
                                f"SQL execution error: {str(sql_error)}", 
                                "The generated SQL query failed to execute.",
                                "There may be an issue with the query syntax or database schema.",
                                actual_session_id, [], enable_ai_insights
                            )
                    else:
                        # No valid SQL generated
                        return {
                            "question": question,
                            "response_type": "error",
                            "analysis": f"I couldn't generate SQL for your question: '{question}'. This appears to be a data question but I wasn't able to create a valid query.",
                            "suggestion": "Try rephrasing your question more specifically, such as 'Show me revenue by business unit' or 'Calculate profit margins for last year'",
                            "timestamp": datetime.now().isoformat(),
                            "session_id": actual_session_id,
                            "ai_insights_enabled": enable_ai_insights
                        }
                        
                else:
                    # Model decided this is conversational
                    analysis = llm_response.split("ANALYSIS:")[-1].strip() if "ANALYSIS:" in llm_response else llm_response
                    return {
                        "question": question,
                        "response_type": "conversational",
                        "analysis": analysis,
                        "timestamp": datetime.now().isoformat(),
                        "session_id": actual_session_id,
                        "ai_insights_enabled": enable_ai_insights
                    }
                    
            except Exception as e:
                logger.error("LLM processing failed", error=str(e))
                return self.create_error_response(f"LLM error: {str(e)}", 
                                                "Failed to process your question with the AI model.",
                                                "Try rephrasing your question more clearly.",
                                                actual_session_id, [], enable_ai_insights)
        
        except Exception as e:
            logger.error("Analysis failed", error=str(e))
            return self.create_error_response(f"Analysis error: {str(e)}",
                                            "I encountered an error while analyzing your question.",
                                            "Try rephrasing your question with more specific details.",
                                            actual_session_id, [], enable_ai_insights)                                      
    
    async def get_simple_conversation_history(self, session_id: str, limit: int = 5) -> List[Dict]:
        """Simple conversation history - let the model handle temporal reasoning"""
        clean_session_id = str(session_id).strip().replace('"', '').replace("'", "")
        
        conversation = []
        try:
            # Get more context (increased from 3 to 5 pairs)
            history_query = f"""
            ChatHistory_CFO
            | where SessionID has "{clean_session_id}"
            | where Question != 'tables_info' and Question != 'schema_info'
            | where Question != ''
            | order by Timestamp desc
            | take {limit * 2}
            | order by Timestamp asc
            | extend 
                Decoded_Response = case(
                    Response startswith "eyJ" or Response startswith "ew", base64_decode_tostring(Response),
                    Response
                )
            | project Question, Decoded_Response, Timestamp
            """
            
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.db_manager.kusto_client.execute(self.db_manager.kusto_database, history_query)
            )
            
            raw_results = result.primary_results[0] if result.primary_results else []
            
            for row in raw_results:
                try:
                    question = row["Question"]
                    decoded_response = row["Decoded_Response"]
                    
                    if not decoded_response:
                        continue
                    
                    response_data = json.loads(decoded_response)
                    
                    # Add user message
                    conversation.append({
                        "role": "user",
                        "content": question
                    })
                    
                    # Add assistant message - INCLUDE THE ORIGINAL SQL for context
                    generated_sql = response_data.get('generated_sql', '')
                    result_count = response_data.get('result_count', 0)
                    analysis = response_data.get('analysis', '')
                    
                    # Simple assistant message that preserves key context
                    assistant_content = f"I found {result_count} records. "
                    if generated_sql:
                        assistant_content += f"I used this query: {generated_sql[:200]}... "
                    assistant_content += f"Analysis: {analysis[:300]}..."
                    
                    conversation.append({
                        "role": "assistant", 
                        "content": assistant_content
                    })
                    
                except Exception:
                    continue
                    
            return conversation
            
        except Exception as e:
            logger.error("Failed to get conversation history", error=str(e))
            return []
    
    def is_casual_greeting(self, question: str) -> bool:
        """Check if question is a casual greeting"""
        vague_questions = ["hi", "hello", "hey", "greetings"]
        return question.lower().strip() in vague_questions
    
    async def handle_casual_greeting(self, question: str, session_id: str, conversation_history: List[Dict], enable_ai_insights: bool) -> Dict[str, Any]:
        """Handle casual greetings"""
        conversational_prompt = f"""The user said: \"{question}\"

This is a casual greeting. Provide a friendly response that:
1. Acknowledges their greeting
2. Explains what this enhanced analytics tool can do
3. Mentions AI-powered insights and email capabilities if available
4. Suggests example questions
5. Invites a specific question"""
        
        conversational_response = await self.ai_services.ask_intelligent_llm_async(conversational_prompt)
        
        return {
            "question": question,
            "response_type": "conversational",
            "analysis": conversational_response,
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "conversation_history": conversation_history,
            "ai_insights_enabled": enable_ai_insights,
            "ai_insights": None
        }
    
    def create_error_response(self, error: str, analysis: str, suggestion: str, session_id: str, conversation_history: List[Dict], enable_ai_insights: bool) -> Dict[str, Any]:
        """Create standardized error response"""
        return {
            "question": "",
            "error": error,
            "analysis": analysis,
            "suggestion": suggestion,
            "session_id": session_id,
            "conversation_history": conversation_history,
            "ai_insights_enabled": enable_ai_insights,
            "ai_insights": None
        }
    
    async def handle_conversational_question(self, question: str, session_id: str, conversation_history: List[Dict], enable_ai_insights: bool) -> Dict[str, Any]:
        """Handle conversational questions"""
        conversational_prompt = f"""The user asked: "{question}"

This doesn't require database analysis. Provide a conversational response that:
1. Addresses the question
2. Explains relevant concepts
3. Offers data analysis help with AI insights
4. Suggests data exploration"""
        
        conversational_response = await self.ai_services.ask_intelligent_llm_async(conversational_prompt)
        
        return {
            "question": question,
            "response_type": "conversational",
            "analysis": conversational_response,
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "conversation_history": conversation_history,
            "ai_insights_enabled": enable_ai_insights,
            "ai_insights": None
        }
    
    async def execute_sql_query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query with proper error handling"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self.db_manager.execute_sql_query(sql))
    
    async def add_enhanced_analysis(self, question: str, sql: str, results: List[Dict], context: Dict, response: Dict, enable_ai_insights: bool):
        """Add enhanced analysis to response"""
        # Standard LLM analysis
        enhanced_prompt = f"""
User Question: {question}

Query Results: {len(results)} records
Generated SQL: {sql}

Sample Data: {json.dumps(results[:10], default=Utils.safe_json_serialize)}

Provide a conversational response that:
1. Summarizes results
2. Explains business context
3. Identifies key patterns
4. Provides actionable recommendations
5. Suggests next steps

Use clear formatting with headers and bullet points. Include specific numbers and percentages from the data.
"""
        
        try:
            standard_analysis = await self.ai_services.ask_intelligent_llm_async(enhanced_prompt)
        except Exception as e:
            standard_analysis = f"Analysis generation failed: {str(e)}"
        
        # Enhanced AI analysis if available
        if enable_ai_insights and self.ai_services.ai_foundry_enabled and self.ai_services.intelligent_agent is not None:
            try:
                ai_insights = await self.ai_services.intelligent_agent.analyze_with_ai(
                    results, question, context
                )
                
                if ai_insights:
                    response["ai_insights"] = ai_insights
                    response["enhanced_analysis"] = f"{standard_analysis}\n\n**🤖 AI-Enhanced Insights:**\n{ai_insights}"
                else:
                    response["ai_insights"] = "AI insights could not be generated; using standard analysis."
                    response["enhanced_analysis"] = standard_analysis
                    
            except Exception as e:
                logger.error("AI insights generation error", error=str(e))
                response["enhanced_analysis"] = standard_analysis
                response["ai_insights"] = f"AI Foundry insights error: {str(e)}"
        else:
            response["enhanced_analysis"] = standard_analysis
            if not enable_ai_insights:
                response["ai_insights"] = "AI insights disabled by user."
            elif not self.ai_services.ai_foundry_enabled:
                response["ai_insights"] = "AI Foundry not enabled; using standard LLM analysis."
            elif self.ai_services.intelligent_agent is None:
                response["ai_insights"] = "AI agent not available; using standard LLM analysis."
            else:
                response["ai_insights"] = "AI Foundry not available; using standard analysis."
    
    async def build_prompt_with_conversation(self, question: str, tables_info: List[Dict], conversation_history: List[Dict]) -> str:
        """Simple prompt - trust the model to handle temporal logic"""
        
        print(f"prompt_manager type: {type(self.prompt_manager)}")
        print(f"Has load_base_prompt: {hasattr(self.prompt_manager, 'load_base_prompt')}")
        print(f"Has format_schema_for_prompt: {hasattr(self.prompt_manager, 'format_schema_for_prompt')}")
    
        
        base_prompt = self.prompt_manager.load_base_prompt()
        schema_section = self.prompt_manager.format_schema_for_prompt(tables_info)
        
        # Simple conversation context
        conversation_section = ""
        if conversation_history:
            conversation_section = "\n\n📝 CONVERSATION HISTORY:\n"
            for msg in conversation_history[-6:]:  # Last 3 Q&A pairs
                role = "User" if msg["role"] == "user" else "Assistant"
                conversation_section += f"{role}: {msg['content']}\n\n"
        
        time_context = f"""
            CURRENT DATE CONTEXT:
            - Today's date: {datetime.now().strftime('%Y-%m-%d')}
            - Current year: {current_year}
            - Default assumption: Use current year ({current_year}) data unless specified otherwise
            - For "recent performance", "current status", "how are we doing" → use {current_year}

            """
        
        # Simple, clear instruction
        sql_instruction = f"""
    🎯 CURRENT QUESTION: "{question}"

    INSTRUCTIONS:
    - Use the conversation history above to understand context and maintain consistency
    - When users refer to "this", "that", "it", or "them", look at the previous questions and data
    - Maintain the same time periods and filters unless explicitly asked to change them
    - Generate SQL that answers the current question in the context of our conversation

    RESPONSE FORMAT:
    SQL_QUERY:
    [Your SQL query]

    ANALYSIS:
    [Your analysis]
    """
        
        return f"{base_prompt}\n\n{time_context}\n{schema_section}\n{conversation_section}\n{sql_instruction}"
    
    def validate_sql_against_schema(self, sql: str, tables_info: List[Dict]) -> tuple[bool, str]:
        """Simple validation - only catch obvious hallucinated table names"""
        if not sql or not tables_info:
            return True, "Validation skipped"  # Don't block execution
        
        # Only check for OBVIOUS hallucinated table names that we know are problematic
        obvious_hallucinations = [
            "Revenue_Growth", "Sales_Performance", "Customer_Analytics", 
            "Business_Metrics", "Financial_Summary", "Performance_Data",
            "Monthly_Report", "Quarterly_Data", "Annual_Stats"
        ]
        
        sql_upper = sql.upper()
        for hallucination in obvious_hallucinations:
            if hallucination.upper() in sql_upper:
                return False, f"SQL contains hallucinated table '{hallucination}' - please use actual table names from the schema"
        
        # If no obvious hallucinations found, let it through
        return True, "Validation passed"

  