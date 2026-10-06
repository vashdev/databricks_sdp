Sub TestDatabricksLogin()
    Dim workspaceUrl As String
    Dim clientId As String
    Dim clientSecret As String
    Dim accessToken As String
    
    ' --- FILL IN YOUR DETAILS HERE ---
    workspaceUrl = "https://<your-workspace-hash>.cloud.databricks.com" ' e.g., https://dbc-a1b2345c-d6e7.cloud.databricks.com
    clientId = "<your-service-principal-client-id>"
    clientSecret = "<your-service-principal-client-secret>"
    ' ---------------------------------
    
    ' Call the authentication function
    accessToken = GetDatabricksOAuthToken(workspaceUrl, clientId, clientSecret)
    
    If accessToken <> "" Then
        MsgBox "Success! Authenticated with Databricks." & vbCrLf & vbCrLf & _
               "Access Token (truncated): " & Left(accessToken, 30) & "...", _
               vbInformation, "Databricks Connected"
        
        ' You can now use this 'accessToken' for subsequent API requests
    Else
        MsgBox "Authentication failed. Check your credentials and workspace URL.", vbCritical, "Failed"
    End If
End Sub

' Core plug-and-play authentication function
Function GetDatabricksOAuthToken(wsUrl As String, cId As String, cSec As String) As String
    Dim http As Object
    Dim tokenUrl As String
    Dim payload As String
    Dim authEncoded As String
    Dim jsonResponse As String
    
    ' Clean trailing slash from workspace URL if provided
    If Right(wsUrl, 1) = "/" Then wsUrl = Left(wsUrl, Len(wsUrl) - 1)
    
    ' Construct standard Databricks OAuth token endpoint
    tokenUrl = wsUrl & "/oidc/v1/token"
    payload = "grant_type=client_credentials&scope=all-apis"
    
    Set http = CreateObject("MSXML2.ServerXMLHTTP.6.0")
    
    On Error GoTo ErrorHandler
    
    http.Open "POST", tokenUrl, False
    http.setRequestHeader "Content-Type", "application/x-www-form-urlencoded"
    
    ' Base64 encode Client ID and Client Secret for HTTP Basic Auth
    authEncoded = EncodeBase64(cId & ":" & cSec)
    http.setRequestHeader "Authorization", "Basic " & authEncoded
    
    http.send payload
    
    If http.Status = 200 Then
        jsonResponse = http.responseText
        ' Extract access_token from the JSON response text
        GetDatabricksOAuthToken = ExtractJsonValue(jsonResponse, "access_token")
    Else
        MsgBox "Databricks API Error (" & http.Status & "): " & http.responseText, vbCritical, "Auth Error"
        GetDatabricksOAuthToken = ""
    End If
    Exit Function

ErrorHandler:
    MsgBox "Runtime Error during authentication: " & Err.Description, vbCritical, "Error"
    GetDatabricksOAuthToken = ""
End Function

' Helper function for Base64 encoding
Private Function EncodeBase64(txt As String) As String
    Dim arrData() As Byte
    arrData = StrConv(txt, vbFromUnicode)
    
    Dim objXML As Object
    Dim objNode As Object
    
    Set objXML = CreateObject("MSXML2.DOMDocument")
    Set objNode = objXML.createElement("b64")
    
    objNode.dataType = "bin.base64"
    objNode.nodeTypedValue = arrData
    EncodeBase64 = Replace(objNode.text, vbLf, "")
    
    Set objNode = Nothing
    Set objXML = Nothing
End Function

' Helper function to lightweight-parse JSON without external libraries
Private Function ExtractJsonValue(json As String, key As String) As String
    Dim pattern As String
    Dim startPos As Long, endPos As Long
    
    pattern = """" & key & """:"""
    startPos = InStr(json, pattern)
    
    If startPos > 0 Then
        startPos = startPos + Len(pattern)
        endPos = InStr(startPos, json, """")
        If endPos > startPos Then
            ExtractJsonValue = Mid(json, startPos, endPos - startPos)
        End If
    End If
End Function