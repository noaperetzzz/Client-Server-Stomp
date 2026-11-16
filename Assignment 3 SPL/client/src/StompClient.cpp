#include <iostream>
#include <ConnectionHandler.h>
#include <string>
#include <event.h>
#include <cstdlib>
#include <fstream>
#include <thread>
#include <sstream>
#include <atomic>
#include <StompProtocol.h>

struct UserGameData {
    std::string team_a;
    std::string team_b;
    std::map<std::string, std::string> general_stats;
    std::map<std::string, std::string> team_a_stats;
    std::map<std::string, std::string> team_b_stats;
    
    std::vector<Event> reports; 
    UserGameData() : team_a(""), team_b(""), general_stats(), team_a_stats(), team_b_stats(), reports() {}
};

std::mutex db_mutex;
std::atomic<bool> shouldTerminate(false);
std::atomic<int> disconnectReceiptId(-1);
std::map <int, std::string> map_subscribe;
std::map <int, std::string> map_unsubscribe;
std::map<std::string, std::map<std::string, UserGameData>> database;
std::atomic<bool> isLoggedIn(false);

void socketReaderTask(ConnectionHandler& handler, std::map<std::string, std::map<std::string, UserGameData>>& database) {
    while (!shouldTerminate) {
        std::string frame;
        if (!handler.getFrameAscii(frame, '\0')) {
            if (!shouldTerminate) {
                std::cout << "Disconnected from server" << std::endl;
            }
            isLoggedIn = false;
            shouldTerminate = true;
            break;
           
        }
        
        if (frame.find("MESSAGE") != std::string::npos) {
        std::stringstream ss(frame);
        std::string line;
        std::string game_name, user, team_a, team_b, event_name, description;
        int time = 0;

        while (std::getline(ss, line) && line != "\r" && line != "") {
            if (line.find("destination:/") == 0) {
                game_name = line.substr(13); 
            }
        }

       
        std::string current_section = ""; 
        
        std::lock_guard<std::mutex> lock(db_mutex);

        while (std::getline(ss, line)) {
            if (line.empty()){
                continue;
            }

            if (line.find("user: ") == 0){
                user = line.substr(6);
            }
            else if (line.find("team a: ") == 0){
                team_a = line.substr(8);
            }
            else if (line.find("team b: ") == 0){
                team_b = line.substr(8);
            }
            else if (line.find("event name: ") == 0){
                event_name = line.substr(12);
            }
            else if (line.find("time: ") == 0){
                time = std::stoi(line.substr(6));
            }
            
            else if (line.find("general game updates:") == 0){
                current_section = "general";
            }
            else if (line.find("team a updates:") == 0){
                current_section = "team_a";
            }
            else if (line.find("team b updates:") == 0){
                current_section = "team_b";
            }
            else if (line.find("description:") == 0) {
            description = line.substr(13); 
            std::string rest;
            std::getline(ss, rest, '\0');
            description += rest;
        }
            
            else if (line[0] == ' ' || line[0] == '\t') {
                size_t colon_pos = line.find(":");
                if (colon_pos != std::string::npos) {
                    std::string key = line.substr(0, colon_pos);

                    key.erase(0, key.find_first_not_of(" \t")); 
                    std::string value = line.substr(colon_pos + 1);
                    value.erase(0, value.find_first_not_of(" \t"));

                    if (current_section == "general"){
                        database[game_name][user].general_stats[key] = value;
                    }
                    else if (current_section == "team_a"){
                        database[game_name][user].team_a_stats[key] = value;
                    }
                    else if (current_section == "team_b"){
                        database[game_name][user].team_b_stats[key] = value;
                    }
                }
            }
        }

        database[game_name][user].team_a = team_a;
        database[game_name][user].team_b = team_b;
        Event event(team_a, team_b, event_name, time, 
            database[game_name][user].general_stats, 
            database[game_name][user].team_a_stats, 
            database[game_name][user].team_b_stats, 
            description);

        database[game_name][user].reports.push_back(event);

        } else if (frame.find("RECEIPT") != std::string::npos && isLoggedIn ) {
            size_t pos = frame.find("receipt-id:");
            if (pos != std::string::npos) {
                int rId = std::stoi(frame.substr(pos + 11));
                
                if (rId == disconnectReceiptId) {
                    std::cout << "Logging out" << std::endl;
                    isLoggedIn = false;
                    shouldTerminate = true;
                    handler.close();
                }
                else if (map_subscribe.count(rId) > 0){
                    std::cout << "Joined channel " << map_subscribe[rId] << std::endl;
                }
                else if (map_unsubscribe.count(rId) > 0){
                    std::cout << "Exited channel " << map_unsubscribe[rId] << std::endl;
                }
            }
            
        } else if (frame.find("CONNECTED") != std::string::npos) {
            isLoggedIn = true;
            std::cout << "Login successful" << std::endl;
        } else if (frame.find("ERROR") != std::string::npos){
            if (frame.find("WRONG_PASSWORD") != std::string::npos || frame.find("Wrong password") != std::string::npos){
                std::cout << "Wrong password" << std::endl;
            }
            else if (frame.find("ALREADY_LOGGED_IN") != std::string::npos || frame.find("CLIENT_ALREADY_CONNECTED") != std::string::npos){
                std::cout << "User already logged in" << std::endl;
            }
            else if (frame.find("Send failed") != std::string::npos){
                std::cout << "Error: Message delivery failed" << std::endl;
            }
            else if (frame.find("User is not connected") != std::string::npos){
                std::cout << "Error: The user is not connected" << std::endl;
            }
            else if (frame.find("SubscriptionId doesn't exists") != std::string::npos){
                std::cout << "Error: The user is not subscribed to this channel" << std::endl;
            }
            std::cout << "Logging out" << std::endl;
            isLoggedIn = false;
            shouldTerminate = true;
            handler.close();
        }
}
}

int main(int argc, char *argv[]) {
    // TODO: implement the STOMP client
    
    std::unique_ptr<ConnectionHandler> handler;
    std::thread readerThread;

    int subscriptionId = 0;
    int receiptId = 70;

    std::string username = "";
    std::map<std::string, int> game_to_sub_id;

    while (1) {

        const short bufsize = 1024;
        char buf[bufsize];
        std::cin.getline(buf, bufsize);
        std::string line(buf);
        std::stringstream ss(line);
        std::string command;
        ss >> command;
        
        if (command == "login") {
            std::string password;
            std::string host_port;
            std::string host;
            short port;
            ss >> host_port;
            ss >> username;
            ss >> password;

            size_t colon_pos = host_port.find(":");
            host = host_port.substr(0, colon_pos); 
            std::string port_str = host_port.substr(colon_pos + 1);
            port = static_cast<short>(std::stoi(port_str));

            if (isLoggedIn) {
                std::cout << "The client is already logged in, log out before trying again" << std::endl;
            }
            else {

                if (readerThread.joinable()) {
                    shouldTerminate = true;
                    if (handler) {
                        handler->close(); 
                    }
                    readerThread.join();
                }

                handler.reset(new ConnectionHandler(host, port));

                shouldTerminate = false;
                disconnectReceiptId = -1;
                game_to_sub_id.clear(); 
                map_subscribe.clear();
                map_unsubscribe.clear();
                
                if (!handler->connect()) {
                    std::cerr << "Could not connect to server" << std::endl;
                }
                else {
                    readerThread = std::thread (socketReaderTask, std::ref(*handler), std::ref(database));
                    if (!handler->sendFrameAscii(StompProtocol::connectFrame(host, username, password), '\0')) {
                        std::cerr << "Could not connect to server" << std::endl;
                    }
                }
            }
        } else if (command == "join") {
            if (!isLoggedIn) {
                std::cout << "Error: For any command (apart from login), you must be logged in." << std::endl;
                continue; 
            }
            std::string game_name;
            ss >> game_name;

            //Case 1: The client is subscribed to this channel
            if (game_to_sub_id.count(game_name)){
                std::cout << "Joined channel " << game_name << std::endl;
            }
            //Case 2: The client is not subscribed to this channel
            else {
                game_to_sub_id[game_name] = subscriptionId;
                map_subscribe[receiptId] = game_name;
            
                database[game_name];

                for (auto it = map_unsubscribe.begin(); it != map_unsubscribe.end(); ) {
                        if (it->second == game_name) {
                            it = map_unsubscribe.erase(it);
                        } else {
                            ++it;
                        }
                }

                handler->sendFrameAscii(StompProtocol::subscribeFrame(game_name, subscriptionId, receiptId), '\0');
                subscriptionId++;
                receiptId++;
                }
        }
        
        else if (command == "exit"){
            std::string game_name;
            ss >> game_name;
            
            std::string unsubscribe_ss;

            //If the client is not subscribed to this channel
            if (!game_to_sub_id.count(game_name)) {
                unsubscribe_ss = StompProtocol::unsubscribeFrame(-1, receiptId);

                handler->sendFrameAscii(unsubscribe_ss, '\0');
            }

            //If the client is subscribed to this channel
            else {
                int sub_id = game_to_sub_id.find(game_name)->second;
                
                map_unsubscribe[receiptId] = game_name;

                unsubscribe_ss = StompProtocol::unsubscribeFrame(sub_id, receiptId);
    
                handler->sendFrameAscii(unsubscribe_ss, '\0');

                for (auto it = map_subscribe.begin(); it != map_subscribe.end(); ) {
                    if (it->second == game_name) {
                        it = map_subscribe.erase(it);
                    } else {
                        ++it;
                    }
                }
                 game_to_sub_id.erase(game_name);
            }
            receiptId++;
        }

        else if(command == "report"){
            std::string json_path;
            ss >> json_path;
            names_and_events names_events = parseEventsFile(json_path);
            std::string team_a_name = names_events.team_a_name;
            std:: string team_b_name = names_events.team_b_name;


            
            for(const Event& event: names_events.events){
                std::string ss_frame = StompProtocol::sendFrame(team_a_name + "_" + team_b_name, username, event, json_path);

                handler->sendFrameAscii(ss_frame, '\0');
            }
        }

        else if (command == "summary"){
            std::string game_name;
            std::string user;
            std::string file;
            ss >> game_name;
            ss >> user;
            ss >> file;
            std::string team_a_name = game_name.substr(0, game_name.find("_"));
            std::string team_b_name = game_name.substr(game_name.find("_") + 1, game_name.size());
            std::stringstream ss_frame;
            ss_frame << team_a_name << " vs " << team_b_name << "\n"
            << "Game stats:" << "\n"
            << "General stats:" << "\n";
            
            std::lock_guard<std::mutex> lock(db_mutex);

            auto game_it = database.find(game_name);

            if (game_it == database.end()) {
                std::cout << "Game " << game_name << " not found in database" << std::endl;
            }
            else {
                auto user_it = game_it->second.find(user);
        
                if (user_it == game_it->second.end()) {
                    std::cout << "No reports from user " << user << " for game " << game_name << std::endl;
                }
                else {
                    for (const auto& stat : database[game_name][user].general_stats){
                        ss_frame << stat.first << ": " << stat.second << "\n";
                    }
                    ss_frame << team_a_name << " stats:\n";
                    for (const auto& stat : database[game_name][user].team_a_stats){
                        ss_frame << stat.first << ": " << stat.second << "\n";
                    }
                    ss_frame << team_b_name << " stats:\n";
                    for (const auto& stat : database[game_name][user].team_b_stats){
                        ss_frame << stat.first << ": " << stat.second << "\n";
                    }
                    ss_frame << "Game event reports:\n";
                    for (Event event : database[game_name][user].reports){
                        ss_frame << event.get_time() << " - " << event.get_name() << ":\n\n"
                        << event.get_discription() << "\n\n";
                    }

                    std::ofstream outFile(file);
                    if (outFile.is_open()) {
                        outFile << ss_frame.str();
                        outFile.close();
                        std::cout << "Summary created successfully in: " << file << std::endl;

            } else {
                std::cerr << "Error: Could not create file " << file << std::endl;
            }
            }
            }
        }
        else if (command == "logout") {
            disconnectReceiptId = receiptId;
            std::string ss_frame;
            ss_frame = StompProtocol::disconnectFrame(receiptId);
            handler->sendFrameAscii(ss_frame, '\0');
            receiptId++;
        }
    }
    readerThread.join();
}
