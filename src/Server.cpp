#include <bits/stdc++.h>
using namespace std;

const int DB_HEADER_SIZE = 100;
const int PAGE_HEADER_SIZE = 8;

uint64_t parse_varint(std::ifstream &is)
{
    char c;
    is.read(&c, 1);
    unsigned char d = static_cast<unsigned char>(c);
    uint64_t ret = 0;
    int length = 1;
    while ((d >> 7 & 1) && length < 9)
    {
        ret = ret << 7 | d & (1 << 7) - 1;
        is.read(&c, 1);
        d = static_cast<unsigned char>(c);
        length++;
    }
    ret = ret << 7 | d;
    return ret;
}

std::string parse_column_value(std::ifstream &stream, int serial_type)
{
    if (serial_type >= 13 && serial_type % 2 == 1)
    {
        int length = (serial_type - 13) / 2;
        char buff[length];
        stream.read(buff, length);
        std::string str(buff, buff + sizeof buff / sizeof buff[0]);
        return str;
    }
    else if (serial_type == 1)
    {
        int length = 1;
        char buff[length];
        stream.read(buff, length);
        unsigned short val = static_cast<unsigned char>(buff[0]);
        return to_string(val);
    }

    return "";
}

std::vector<std::string> parse_record(std::ifstream &stream, int column_count)
{
    int _num_bytes_in_header = parse_varint(stream);
    std::vector<int> serial_types;
    for (int i = 0; i < column_count; i++)
    {
        serial_types.push_back(parse_varint(stream));
    }
    std::vector<std::string> values;
    for (int i = 0; i < serial_types.size(); i++)
    {
        values.push_back(parse_column_value(stream, serial_types[i]));
    }
    return values;
}

uint64_t big_endian(ifstream &is, int length)
{
    char c;
    is.read(&c, 1);
    unsigned char d = static_cast<unsigned char>(c);
    int len = 1;
    uint64_t ret = 0;
    while (len <= length)
    {
        ret = ret << 8 | d;
        is.read(&c, 1);
        d = static_cast<unsigned char>(c);
        len++;
    }
    return ret;
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

    int get_table_rows_count(string table_name)
    {
        for (auto row : master_table_rows)
        {
            if (row.name == table_name)
            {
                this->stream.seekg((row.root_page - 1) * this->page_size + 3);
                int cell_count = big_endian(this->stream, 2);
                return cell_count;
            }
        }
        return 0;
    }

    void query_all_rows(string table, vector<string> query_columns)
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
                assert(query_column_positions.size() == query_columns.size());
                int offset = (row.root_page - 1) * this->page_size;
                this->stream.seekg(offset);
                int page_type = big_endian(this->stream, 1);
                assert(page_type == 13);
                this->stream.seekg(offset + 3);
                unsigned short cell_count = big_endian(this->stream, 2);
                for (int i = 0; i < cell_count; i++)
                {
                    this->stream.seekg(offset + PAGE_HEADER_SIZE + (i * 2));
                    unsigned short curr_cell_location = big_endian(this->stream, 2);
                    this->stream.seekg(offset + curr_cell_location);
                    parse_varint(this->stream);
                    parse_varint(this->stream);
                    
                    std::vector<std::string> row = parse_record(this->stream, table_columns.size());
                    for (int j = 0; j < query_column_positions.size(); j++)
                    {
                        if (j == 0)
                        {
                            cout << row[query_column_positions[j]];
                        }
                        else
                        {
                            cout << "|" << row[query_column_positions[j]];
                        }
                    }
                    cout << endl;
                }
                break;
            }
        }
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
        vector<string> tokens = tokenize(command, " ");

        if (command.rfind("COUNT") != string::npos || command.rfind("count") != string::npos)
        {
            string table_to_query = tokens.back();
            cout << db.get_table_rows_count(table_to_query) << endl;
        }
        else
        {
            vector<string> columns;
            for (int i = 1; i < tokens.size(); i++)
            {
                if (get_lowercase(tokens[i]) == "from")
                {
                    break;
                }
                if (tokens[i].back() == ',')
                {
                    columns.push_back(tokens[i].substr(0, tokens[i].size() - 1));
                }
                else
                {
                    columns.push_back(tokens[i]);
                }
            }
            db.query_all_rows(tokens.back(), columns);
        }
    }
    return 0;
}

