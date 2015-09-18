class SimpleDatabase
  attr_reader :file_path, :table

  def initialize(file_path)
    @file_path   ||= file_path
    @table       ||= {}
    #key is value in table, value is the apperance count
    @count_table ||= {}
  end

  def set(key, value)
    table[key.to_sym] = value
  end

  def get(key)
    table[key.to_sym]
  end

  def unset(key)
    table.delete[key.to_sym]
  end

  def numequalto(value)
    table.select{  }.count
  end

  def begin
  end

  def rollback
  end

  def commit
  end

  def print_data
    table.each { |key, value| puts "#{key}: #{value}" }
  end

  def load_data
    File.open(file_path) do |f|
      f.each_line do |line|
        key, value = parsed_entry(line)
        table[key.to_sym] = value
      end
    end
  end

  private

  def parsed_entry(line)
    key, value = line.split(" ")
    return key, value
  end
end
