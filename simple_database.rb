class SimpleDatabase
  attr_reader :file_path, :data_table, :count_table, :dir_path_to_data, :dir_path_to_count

  def initialize(file_path, dir_path_to_data, dir_path_to_count)
    @dir_path_to_data  ||= dir_path_to_data
    @dir_path_to_count ||= dir_path_to_count
    @file_path         ||= file_path
    @data_table        ||= {}
    @count_table       ||= {}
  end

  def set(key, value=1, is_data=true)
    if is_data
      data_table[key.to_sym] = value.to_s
    else
      return if key.nil?

      if count_table[key.to_sym]
        count_table[key.to_sym] = (value.to_i + count_table[key.to_sym].to_i).to_s
      else
        count_table[key.to_sym] = value.to_s
      end
    end
  end

  def get(key, is_data=true)
    if is_data
      data_table[key.to_sym]
    else
      return 0 if key.nil?

      count_table[key.to_sym] ? count_table[key.to_sym] : 0
    end
  end

  def unset(key, is_data=true)
    if is_data
      data_table.delete(key.to_sym)
    else
      if count_table[key.to_sym]
        count_table[key.to_sym] = (count_table[key.to_sym].to_i - 1).to_s
      else
        count_table[key.to_sym] = 0
      end
    end
  end

  def delete_data_file(key)
    full_path = "#{dir_path_to_data}#{key}.txt"
    File.delete(full_path)
  end

  def create_or_update_file(key, value, is_data=true)
    full_path = if is_data
      "#{dir_path_to_data}#{key}.txt"
    else
      "#{dir_path_to_count}#{key}.txt"
    end

    File.open(full_path, 'w') { |f| f.write(value) }
  end

  def load_db
    load(dir_path_to_data)
    load(dir_path_to_count, false)
  end

  def print_data
    puts "data_table: "
    data_table.each { |key, value| puts "#{key}: #{value}" }
    puts "count_table: "
    count_table.each { |key, value| puts "#{key}: #{value}" }
  end

  private

  def load(dir_path, is_data=true)
    Dir.foreach(dir_path) do |fname|
      next if fname == '.' or fname == '..'
      path = "#{dir_path}#{fname}"
      key = fname.split(".")[0]

      if is_data
        data_table[key.to_sym] = file_content(path)
      else
        count_table[key.to_sym] = file_content(path)
      end
    end
  end

  def file_content(path_to_data)
    File.open(path_to_data) { |f| f.readline }
  end
end
