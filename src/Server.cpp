#include <bits/stdc++.h>
using namespace std;
const int DB_HEADER_SIZE = 100;
const int PAGE_HEADER_SIZE = 8;

uint64_t parse_varint(ifstream &stream)
{
    char c;
stream.read(&c, 1);
    unsigned char d = static_cast<unsigned char>(c);
    uint64_t ret = 0;
    int length = 1;
    while ((d >> 7 & 1) && length < 9)
    {
        ret = ret << 7 | d & (1 << 7) - 1;
stream.read(&c, 1);
        d = static_cast<unsigned char>(c);
        length++;
    }
    ret = ret << 7 | d;
    return ret;
}
pair<uint64_t, int> parse_varint_2(ifstream &stream)
{
    char c;
    stream.read(&c, 1);
    unsigned char d = static_cast<unsigned char>(c);
    uint64_t ret = 0;
    int length = 1;
    while ((d >> 7 & 1) && length < 9)
    {
        ret = ret << 7 | d & (1 << 7) - 1;
        stream.read(&c, 1);
        d = static_cast<unsigned char>(c);
        length++;
    }
    ret = ret << 7 | d;
    return make_pair(ret, length);
}
uint64_t big_endian(ifstream &stream, int length)
{
    char c;
    int len = 0;
    uint64_t ret = 0;
    while (len < length)
    {
stream.read(&c, 1);
        unsigned char d = static_cast<unsigned char>(c);
        ret = ret << 8 | d;
        len++;
    }
    return ret;
}
std::string parse_column_value(ifstream &stream, int serial_type)
{
    if (serial_type == 0)
    {
        return "";
    }
    else if (serial_type <= 4)
    {
        uint32_t x = big_endian(stream, serial_type);
        return to_string(x);
    }
    else if (serial_type == 5)
    {
        return to_string(big_endian(stream, 6));
    }
    else if (serial_type == 6)
    {
        return to_string(big_endian(stream, 8));
    }
    else if (serial_type == 8)
    {
        return to_string(0);
    }
    else if (serial_type == 9)
    {
        return to_string(1);
    }
    else if (serial_type >= 13 && serial_type % 2 == 1)
    {
        int length = (serial_type - 13) / 2;
        char buff[length];
        stream.read(buff, length);
        std::string str(buff, buff + sizeof buff / sizeof buff[0]);
        return str;
    }
    else
    {
        cout << "Not implemented: " << serial_type << endl;
    }
    return "";
}
std::vector<std::string> parse_record(ifstream &stream, int column_count)
{
    int _num_bytes_in_header = parse_varint(stream);
std::vector<uint64_t> serial_types;
    for (int i = 0; i < column_count; i++)
    {
        serial_types.push_back(parse_varint(stream));
    }
    std::vector<std::string> values;
    assert(column_count == serial_types.size());
    for (int i = 0; i < serial_types.size(); i++)
    {
        values.push_back(parse_column_value(stream, serial_types[i]));
    }
    return values;
}
vector<string> tokenize(string s, string delimiter)
{
    size_t pos = 0;
    string token;
    vector<string> tokens;
    while ((pos = s.find(delimiter)) != string::npos)
    {
        token = s.substr(0, pos);
        tokens.push_back(token);
        s.erase(0, pos + delimiter.length());
    }
    if (s.size() > 0)
    {
        tokens.push_back(s);
    }
    return tokens;
}
vector<string> get_columns_from_sql(string sql)
{
    int st_pos = sql.find('(');
    int en_pos = sql.find(')');
    vector<string> columns;
    vector<string> tokens = tokenize(sql.substr(st_pos + 1, en_pos - st_pos - 1), ",");
    for (auto token : tokens)
    {
        vector<string> curr_column = tokenize(token, " ");
        for (string x : curr_column)
        {
            x.erase(remove(x.begin(), x.end(), ' '), x.end());
            x.erase(remove(x.begin(), x.end(), '\n'), x.end());
            x.erase(remove(x.begin(), x.end(), '\t'), x.end());
            if (x.size() > 0)
            {
                columns.push_back(x);
                break;
            }
        }
    }
    return columns;
}
string get_lowercase(string data)
{
    transform(data.begin(), data.end(), data.begin(), [](unsigned char c)
              { return std::tolower(c); });
    return data;
}
class MasterTableRow
{
public:
    string type;
    string name;
    int root_page;
    string sql;
    MasterTableRow(string type, string name, int root_page, string sql)
    {
        this->type = type;
        this->name = name;
        this->root_page = root_page;
        this->sql = sql;
    }
};
class DB
{
public:
    ifstream stream;
    unsigned short page_size;
    vector<MasterTableRow> master_table_rows;
    DB(string db_file_path)
    {
        this->stream = ifstream(db_file_path, ios::binary);
        this->stream.seekg(16);
        this->page_size = big_endian(this->stream, 2);
        this->set_master_table_rows();
    }
    void set_master_table_rows()
    {
        this->stream.seekg(DB_HEADER_SIZE + 3);
        unsigned short cell_count = big_endian(this->stream, 2);
        for (int i = 0; i < cell_count; i++)
        {
            this->stream.seekg(DB_HEADER_SIZE + PAGE_HEADER_SIZE + (i * 2));
            unsigned short curr_cell_location = big_endian(this->stream, 2);
            this->stream.seekg(curr_cell_location);
            parse_varint(this->stream);
            parse_varint(this->stream);
            std::vector<std::string> row = parse_record(this->stream, 5);
            master_table_rows.push_back(MasterTableRow(row[0], row[1], stoi(row[3]), row[4]));
        }
    }
    unsigned short get_number_of_db_tables()
    {
        int cnt = 0;
        for (auto row : master_table_rows)
        {
            if (row.type == "table")
            {
                cnt++;
            }
        }
        return cnt;
    }
    void print_of_db_table_names()
    {
        for (auto row : master_table_rows)
        {
            cout << row.name << " ";
        }
        cout << endl;
    }
    uint64_t count_query_helper(uint64_t page_number){
        uint64_t offset = (page_number - 1) * this->page_size;
        this->stream.seekg(offset);
        unsigned short page_type = big_endian(this->stream, 1);
        uint64_t cnt = 0;
        if (page_type == 13)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);
            return cell_count;
        }
        else if (page_type == 5)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);
            this->stream.seekg(offset + 5);
            unsigned short cell_content_area = big_endian(this->stream, 2);
            this->stream.seekg(offset + 8);
            uint64_t right_page_number = big_endian(this->stream, 4);
            this->stream.seekg(offset + cell_content_area);
            stack<uint64_t> leafs;
            leafs.push(right_page_number);
            for (int i = 0; i < cell_count; i++)
            {
                uint64_t left_page_number = big_endian(this->stream, 4);
                leafs.push(left_page_number);
                parse_varint(this->stream);
            }
            while (!leafs.empty())
            {
                cnt += count_query_helper(leafs.top());
                leafs.pop();
            }
        }
        return cnt;
    }
    void get_table_rows_count(string table_name)
    {
        for (auto row : master_table_rows)
        {
            if (row.name == table_name)
            {
                cout<<count_query_helper(row.root_page)<<endl;
                break;
            }
        }
}
    void traverse_pages(uint64_t page_number, int table_column_cnt, vector<int> query_column_positions, int condition_column_position, string condition_value)
    {
        uint64_t offset = (page_number - 1) * this->page_size;
        this->stream.seekg(offset);
        unsigned short page_type = big_endian(this->stream, 1);
        if (page_type == 13)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);
            for (int i = 0; i < cell_count; i++)
            {
                this->stream.seekg(offset + PAGE_HEADER_SIZE + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);
                parse_varint(this->stream);
                uint64_t rowid = parse_varint(this->stream);
                vector<string> row = parse_record(this->stream, table_column_cnt);
                if (condition_column_position != -1 && row[condition_column_position] != condition_value)
                {
                    continue;
                }
                for (int j = 0; j < query_column_positions.size(); j++)
                {
                    if (j == 0)
                    {
                        string x = row[query_column_positions[j]];
                        if (x.size() == 0)
                        {
                            cout << rowid;
                        }
                        else
                        {
                            cout << x;
                        }
                    }
                    else
                    {
                        cout << "|" << row[query_column_positions[j]];
                    }
                }
                cout << endl;
            }
        }
        else if (page_type == 5)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);
            this->stream.seekg(offset + 5);
            unsigned short cell_content_area = big_endian(this->stream, 2);
            this->stream.seekg(offset + 8);
            uint64_t right_page_number = big_endian(this->stream, 4);
            this->stream.seekg(offset + cell_content_area);
            stack<uint64_t> leafs;
            leafs.push(right_page_number);
            for (int i = 0; i < cell_count; i++)
            {
                uint64_t left_page_number = big_endian(this->stream, 4);
                leafs.push(left_page_number);
                parse_varint(this->stream);
            }
            while (!leafs.empty())
            {
                traverse_pages(leafs.top(), table_column_cnt, query_column_positions, condition_column_position, condition_value);
                leafs.pop();
            }
        }
    }
    void query_by_row_id(uint64_t page_number, string query_row_id, vector<int>query_column_positions, int table_column_cnt){
        uint64_t offset = (page_number - 1) * this->page_size;
        this->stream.seekg(offset);
        unsigned short page_type = big_endian(this->stream, 1);
        if (page_type == 13)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);
            for (int i = 0; i < cell_count; i++)
            {
                this->stream.seekg(offset + PAGE_HEADER_SIZE + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);
                parse_varint(this->stream);
                uint64_t rowid = parse_varint(this->stream);
                
                if(query_row_id != to_string(rowid)){
                    continue;
                }
                vector<string> row = parse_record(this->stream, table_column_cnt);
                for (int j = 0; j < query_column_positions.size(); j++)
                {
                    if (j == 0)
                    {
                        string x = row[query_column_positions[j]];
                        if (x.size() == 0)
                        {
                            cout << rowid;
                        }
                        else
                        {
                            cout << x;
                        }
                    }
                    else
                    {
                        cout << "|" << row[query_column_positions[j]];
                    }
                }
                cout << endl;
                break;
            }
        }
        else if (page_type == 5)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);
            this->stream.seekg(offset + 8);
            uint64_t right_page_number = big_endian(this->stream, 4);
            bool found = false;
            for (int i = 0; i < cell_count; i++)
            {
                this->stream.seekg(offset + PAGE_HEADER_SIZE + 4 + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);
                uint64_t left_page_number = big_endian(this->stream, 4);
                uint64_t row_id = parse_varint(this->stream);
                if(stoull(query_row_id) <= row_id){
                    found = true;
                    query_by_row_id(left_page_number, query_row_id, query_column_positions, table_column_cnt);
                    break;
                }
            }
            if(!found){
                query_by_row_id(right_page_number, query_row_id, query_column_positions, table_column_cnt);
            }
        }
    }
    void get_row_ids(uint64_t page_number, string key, vector<string> &row_ids){
        uint64_t offset = (page_number - 1) * this->page_size;
        this->stream.seekg(offset);
        unsigned short page_type = big_endian(this->stream, 1);
        if(page_type == 2){
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);
            
            this->stream.seekg(offset + 8);
            uint64_t right_page_number = big_endian(this->stream, 4);
            bool found = false;
            for(int i=0;i<cell_count;i++){
                this->stream.seekg(offset + PAGE_HEADER_SIZE  + 4 + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);
                uint64_t left_page_number = big_endian(this->stream, 4);
                parse_varint(this->stream);
                
                auto x = parse_varint_2(this->stream);
                int lim = x.first - x.second;
                vector<uint64_t>serial_types;
                for(int i=0;i<lim;i++){
                    serial_types.push_back(parse_varint(this->stream));
                }
                string curr_key = parse_column_value(this->stream, serial_types[0]);
                if(key <= curr_key){
                    found = true;
                    get_row_ids(left_page_number, key, row_ids);
                    break;
                }
            }
            if(!found){
                get_row_ids(right_page_number, key, row_ids);
            }
        }
        else if(page_type == 10)
        {
            this->stream.seekg(offset + 3);
            unsigned short cell_count = big_endian(this->stream, 2);
            for(int i=0;i<cell_count;i++){
                this->stream.seekg(offset + PAGE_HEADER_SIZE + (i * 2));
                unsigned short curr_cell_location = big_endian(this->stream, 2);
                this->stream.seekg(offset + curr_cell_location);
                parse_varint(this->stream);
                auto x = parse_varint_2(this->stream);
                int lim = x.first - x.second;
                vector<uint64_t>serial_types;
                for(int i=0;i<lim;i++){
                    serial_types.push_back(parse_varint(this->stream));
                }
                string curr_key = parse_column_value(this->stream, serial_types[0]);
                string row_id = parse_column_value(this->stream, serial_types[1]);
                if(curr_key == key){
                    row_ids.push_back(row_id);
                }
                        
            }
        }
    }
    void traverse_index(string key, uint64_t page_number, vector<int>query_column_positions, int table_column_cnt){
        string idx_name = "idx_companies_country";
        uint64_t idx_root_page;
        for(auto row: master_table_rows){
            if(row.name == idx_name){
                idx_root_page = row.root_page;
                break;
            }
        }
        vector<string>row_ids;
        get_row_ids(idx_root_page, key, row_ids);
        
        for(auto x: row_ids){
            query_by_row_id(page_number, x, query_column_positions, table_column_cnt);
        }
    }
    void query_rows(string table, vector<string> query_columns, string condition_column, string condition_value, bool use_index=false)
    {
        for (auto row : master_table_rows)
        {
            if (row.name == table)
            {
                vector<string> table_columns = get_columns_from_sql(row.sql);
                vector<int> query_column_positions;
                for (int i = 0; i < query_columns.size(); i++)
                {
                    for (int j = 0; j < table_columns.size(); j++)
                    {
                        if (get_lowercase(query_columns[i]) == get_lowercase(table_columns[j]))
                        {
                            query_column_positions.push_back(j);
                            break;
                        }
                    }
                }
                if(use_index){
                    traverse_index(condition_value, row.root_page, query_column_positions, table_columns.size());
                    return;
                }
                int condition_column_position = -1;
                if (condition_column.size() > 0)
                {
                    for (int j = 0; j < table_columns.size(); j++)
                    {
                        if (get_lowercase(condition_column) == get_lowercase(table_columns[j]))
                        {
                            condition_column_position = j;
                            break;
                        }
                    }
                }
                assert(query_column_positions.size() == query_columns.size());
                traverse_pages(row.root_page, table_columns.size(), query_column_positions, condition_column_position, condition_value);
                break;
            }
        }
    }
};
class QueryParser
{
public:
    bool is_count_query = false;
    string query_table;
    vector<string> query_columns;
    string condition_column = "", condition_value = "";
    QueryParser(string query)
    {
        if (query.find("count(*)") != string::npos || query.find("COUNT(*)") != string::npos)
        {
            is_count_query = true;
            query_table = tokenize(query, " ").back();
        }
        else
        {
            if (query.find("where") != string::npos || query.find("WHERE") != string::npos)
            {
                int p = query.find("WHERE");
                if (p == string::npos)
                {
                    p = query.find("where");
                }
                string x = query.substr(0, p - 0);
                string y = query.substr(p, query.size() - p + 1);
                vector<string> tokens = tokenize(x, " ");
                bool is_query_coulmn_end = false;
                for (int i = 1; i < tokens.size(); i++)
                {
                    if (get_lowercase(tokens[i]) == "from")
                    {
                        is_query_coulmn_end = true;
                        query_table = tokens[i + 1];
                        break;
                    }
                    if (!is_query_coulmn_end)
                    {
                        if (tokens[i].back() == ',')
                        {
                            query_columns.push_back(tokens[i].substr(0, tokens[i].size() - 1));
                        }
                        else
                        {
                            query_columns.push_back(tokens[i]);
                        }
                    }
                }
                if (y.size() > 0)
                {
                    int z = y.find("=");
                    string col = y.substr(5, z - 5);
                    string val = y.substr(z + 1, y.size() - z - 1);
                    col.erase(remove(col.begin(), col.end(), ' '), col.end());
                    int k = val.find("'");
                    val = val.substr(k + 1, val.size() - k - 2);
                    condition_column = col;
                    condition_value = val;
                }
            }
            else
            {
                vector<string> tokens = tokenize(query, " ");
                bool is_query_coulmn_end = false;
                for (int i = 1; i < tokens.size(); i++)
                {
                    if (get_lowercase(tokens[i]) == "from")
                    {
                        is_query_coulmn_end = true;
                        query_table = tokens[i + 1];
                        break;
                    }
                    if (!is_query_coulmn_end)
                    {
                        if (tokens[i].back() == ',')
                        {
                            query_columns.push_back(tokens[i].substr(0, tokens[i].size() - 1));
                        }
                        else
                        {
                            query_columns.push_back(tokens[i]);
                        }
                    }
                }
            }
        }
    }
    void print_query(){
        cout<<"Query Table: "<<query_table<<endl;
        cout<<"Is Count Query: "<<is_count_query<<endl;
        cout<<"Query Columns: ";
        for(auto x: query_columns){cout<<x<<" ";}
        cout<<endl;
        cout<<"Condition Column: "<<condition_column<<endl;
        cout<<"Condition Value: "<<condition_value<<endl;
    }
};
int main(int argc, char *argv[])
{
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    if (argc != 3)
    {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }
    std::string database_file_path = argv[1];
    std::string command = argv[2];
    DB db(database_file_path);
    if (command == ".dbinfo")
    {
        cout << "database page size: " << db.page_size << endl;
        cout << "number of tables: " << db.get_number_of_db_tables() << endl;
        return 0;
    }
    else if (command == ".tables")
    {
        db.print_of_db_table_names();
        return 0;
    }
    else
    {
        QueryParser qp(command);
        if(qp.is_count_query){
            db.get_table_rows_count(qp.query_table);
        }else{
            bool use_index = false;
            if(qp.condition_column == "country"){
                use_index = true;
            }
            db.query_rows(qp.query_table, qp.query_columns, qp.condition_column, qp.condition_value, use_index);
        }
    }
    return 0;
}