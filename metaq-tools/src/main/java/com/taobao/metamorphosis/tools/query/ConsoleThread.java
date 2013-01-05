package com.taobao.metamorphosis.tools.query;

import java.util.Scanner;

public class ConsoleThread extends Thread {
	Scanner scanner = null;
	Query query = null;
	CommandParser parser = null;
	
	public ConsoleThread(Query query) {
		setName("ConsoleTrehad");
		this.query = query;
		parser = new CommandParser(query);
		scanner = new Scanner(System.in);
	}
	
	public void run() {
		String command = null;
		while(true){
			print();
			command = scanner.nextLine();
			int result = parser.prase(command);
			if(result == -1){
				break;
			}
		}
	}
	
	private void print(){
		System.out.print("meta shell : ");
	}

}
