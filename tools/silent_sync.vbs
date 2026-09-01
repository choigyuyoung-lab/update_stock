Set fso = CreateObject("Scripting.FileSystemObject")
toolsDir = fso.GetParentFolderName(WScript.ScriptFullName)
projDir = fso.GetParentFolderName(toolsDir)
workspaceDir = fso.GetParentFolderName(projDir)

syncScript = workspaceDir & "\k_all_round_portfolio\tools\sync_manager.py"
logFile = workspaceDir & "\update_stock\data\sync_finish.log"

Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd.exe /c python """ & syncScript & """ finish >> """ & logFile & """ 2>&1", 0, True