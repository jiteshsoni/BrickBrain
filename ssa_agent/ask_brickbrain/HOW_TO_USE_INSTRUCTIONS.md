# How to Use BrickBrain Agent Instructions

## 📋 File Purpose

**`brickbrain_agent_instructions.txt`** - System instructions for the AskBrickBrain Databricks Agent

## 🎯 Where to Use This

### In Databricks Agent Configuration UI:

1. Go to your Databricks workspace
2. Navigate to **Apps** → **AskBrickBrain** 
3. In the **Basic Info** section, find the **Description** field
4. Copy the entire contents of `brickbrain_agent_instructions.txt`
5. Paste it into the Description field

### What It Contains:

- **Role definition**: Databricks Knowledge Assistant scope
- **Core rules**: Source of truth, no hallucination, citation requirements
- **Answering procedure**: Step-by-step how the agent should respond
- **Output format**: Structured response format
- **Refusal templates**: How to handle unknown/out-of-scope questions

## 🔧 When to Update

Update this file when you want to:
- Change the agent's behavior or tone
- Add new rules or constraints
- Modify the output format
- Adjust the scope of topics
- Update citation requirements

After updating, paste the new instructions back into the Databricks Agent Description field.

## 📝 Current Agent Configuration

- **Name**: AskBrickBrain
- **Endpoint**: ka-1ff550b7-endpoint
- **MLflow Experiment**: ka-1ff550b7-dev-experiment
- **Scope**: Databricks platform topics only
- **Style**: Brutally honest, concise, precise, directive

---

**Note**: The Description field in Databricks Agent UI is where you paste these system instructions to control the agent's behavior.

