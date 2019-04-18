            String mobile = "13764751332";//Dts.Variables["mobile"].Value.ToString();
            String smstext = "test from ssis";// Dts.Variables["content"].Value.ToString();
            String token = "fe9627b9aaf9ff4233fc50c271173c8fec4719eb31a63d354e5f19013ca304ac";

            byte[] bytes = System.Text.Encoding.UTF8.GetBytes("{\"msgtype\": \"text\", \"text\": {\"content\": \""+smstext+"\"}, \"at\": {\"atMobiles\": [" + mobile + "], \"isAtAll\": false } }");
            string key = BitConverter.ToString(bytes).Replace("-", "").ToLower();

            String url = "https://oapi.dingtalk.com/robot/send?access_token=" + token;
            WebRequest req = HttpWebRequest.Create(url);
            req.Method = "POST";
            req.ContentType = "application/json; charset=utf-8";
            req.ContentLength = bytes.Length;
            Stream dataStream = req.GetRequestStream();
            dataStream.Write(bytes, 0, bytes.Length);
            dataStream.Close();

            WebResponse res = req.GetResponse();
            StreamReader reader = new StreamReader(res.GetResponseStream());
            String result = reader.ReadToEnd();
            Dts.Variables["result"].Value = result;
            Dts.Variables["url"].Value = url;
            MessageBox.Show(result);
            Dts.TaskResult = (int)ScriptResults.Success;
