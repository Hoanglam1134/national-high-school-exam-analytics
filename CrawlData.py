import requests
from bs4 import BeautifulSoup
import pandas as pd

diemthi_data = pd.DataFrame(columns=["sbd", "Toan", "Van", "Anh", "Ly","Hoa","Sinh","KHTN","Su","Dia","GDCD","KHXH"])
sbd = 11000001
while sbd <= 64006588:
	
	URL = "https://thptquocgia.edu.vn/diemthi/?sbd="+str(sbd)
	page = requests.get(URL)
	soup = BeautifulSoup(page.content, "html.parser")
	
	table = soup.find("table",class_="table table-striped table-bordered table-hover responsive-table")
	if table is not None:
		for row in table.tbody.find_all("tr"):
			col = row.find_all("td")
			Toan = col[0].string
			Van = col[1].string
			Anh = col[2].string
			Ly = col[3].string
			Hoa = col[4].string
			Sinh = col[5].string
			KHTN = col[6].string
			Su = col[7].string
			Dia = col[8].string
			GDCD = col[9].string
			KHXH = col[10].string
			diemthi_data = diemthi_data.append({"sbd":sbd, "Toan":Toan, "Van":Van, "Anh":Anh, "Ly":Ly, "Hoa":Hoa, "Sinh":Sinh,"KHTN":KHTN,"Su": Su,"Dia":Dia,"GDCD":GDCD,"KHXH":KHXH}, ignore_index=True)
			diemthi_data.to_csv('draft_1.csv')
			print(diemthi_data.iloc[-1:])
	sbd += 1
