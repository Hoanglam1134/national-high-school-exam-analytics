import requests
from bs4 import BeautifulSoup
import pandas as pd
import multiprocessing as mp

#proxy
from fake_useragent import UserAgent
import random
from fp.fp import FreeProxy

minArr = []
maxArr = []
proxies_list = []

def slice_data(min, max, num_processes):
	part = (max - min + 1) // num_processes
	for i in range(num_processes):
		minArr.append(min + i * part)	
		maxArr.append(min + (i + 1) * part - 1)


def list_proxy(num):
	print('Listing proxies...')
	for i in range (num):
		proxy = FreeProxy(rand=True).get()
		proxies_list.append({'https': proxy})
	print('get proxies_list done')

def crawl(min, max, idx, proxies):
	diemthi_data = pd.DataFrame(columns=["sbd", "Toan", "Van", "Anh", "Ly","Hoa","Sinh","KHTN","Su","Dia","GDCD","KHXH"])
	sbd = min
	file_name = 'data_' + str(idx) + '.csv'
	while sbd <= max:
		if len(proxies)==0:
			print('proxies_list is empty')
			return
		proxy_idx = random.randint(0, len(proxies)-1)
		URL = "https://thptquocgia.edu.vn/diemthi/?sbd="+str(sbd)
		proxy = proxies[proxy_idx]
		page = requests.post(URL, proxies=proxy)
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
				print(diemthi_data.iloc[-1:])
				diemthi_data.to_csv(file_name)
		sbd += 1

if __name__ == '__main__':
	num_processes = 4 #8
	min = 10000001
	max = 64006588
	num_proxies = 5
	list_process = []
	slice_data(min, max, num_processes)
	list_proxy(num_proxies)
	for i in range(num_processes):
		process = mp.Process(target=crawl, args=(minArr[i], maxArr[i], i+1, proxies_list))
		process.start()
		list_process.append(process)

	for process in list_process:
		process.join()
