class CommandBuffer
  attr_accessor :data_change_history, :count_change_history, :blocks, :affected_data, :block_counter

  def initialize
    @data_change_history  ||= {}
    @count_change_history ||= {}
    @affected_data        ||= []
    @blocks ||= []
    @block_counter = -1
  end

  def process(operation, key, value)
    if @block_counter == -1 && operation != "BEGIN"
      if operation == "GET" || operation == "NUMEQUALTO"
        puts CommandExecuter.new(operation, key, value).excute
      else
        CommandExecuter.new(operation, key, value).excute
      end
    else
      if operation == "SET" || operation == "UNSET"
        in_transaction_process(operation, key, value, @block_counter)
      elsif operation == "GET"
        puts "data change history is emtpy" if data_change_history.nil?
        puts get_from_buffer(key)
      elsif operation == "N"
        puts "the key for numequalto_from_buffer is #{key}"
        puts numequalto_from_buffer(key)
      elsif operation == "COMMIT"
        commit
        @block_counter = -1

      elsif operation == "ROLLBACK"
        if @block_counter == -1
          "NO TRANSACTION"
        else
          rollback
          @block_counter -= 1
        end
      elsif operation == "BEGIN"
        @block_counter += 1
        puts "current count: #{@block_counter}"
      end
    end
  end

  def in_transaction_process(operation, key, value, block_index)
    puts "data_change_history: #{data_change_history}"
    puts "count_change_history: #{count_change_history}"
    puts "affected_data: for block_index: #{block_index} key: #{key} #{affected_data[block_index]}"    

    affected_data[block_index] = [] if affected_data[block_index].nil?
    affected_data[block_index] |= [key]

    data_change_history[key.to_sym] = [] if data_change_history[key.to_sym].nil?
    history_for_current_key = data_change_history[key.to_sym]
    data_previous_index = history_for_current_key.empty? ? -1 : history_for_current_key.last[:block_index]


    #grab original value for current key
    original_value = get_from_buffer(key)
    puts "original_value: #{original_value.inspect}, data_previous_index, #{data_previous_index} "
    #decrement the count for current key in buffer, if it is not nil
    if original_value.nil?
      #do nothing because the original didn't exist
    else
      count_change_history[original_value.to_sym] = [] if count_change_history[original_value.to_sym].nil?
      history_for_original_value = count_change_history[original_value.to_sym]
      count_previous_index = history_for_original_value.empty? ? -1 : history_for_original_value.last[:block_index]
      last_change = history_for_original_value.empty? ? 0 : history_for_original_value.last[:diff]
      if block_index != count_previous_index
        #creates a new entry in history
        count_change_history[original_value.to_sym] << { diff: last_change - 1, block_index: block_index } 
      else
        #if the same block, the new values overwrites the last history
        count_change_history[original_value.to_sym].last[:diff] = last_change - 1
      end
    end

    #update data history for current key
    if block_index != data_previous_index
      data_change_history[key.to_sym] << { value: value, block_index: block_index }  
    else
      data_change_history[key.to_sym].last[:value] = value
    end

    #grab new value for current key
    new_value = value

    #increment the count for the new key in buffer, if it is not nil
    if new_value.nil?
      #do nothing because the new value is nil, don't need to track
    else
      count_change_history[new_value.to_sym] = [] if count_change_history[new_value.to_sym].nil?
      history_for_new_value = count_change_history[new_value.to_sym]
      count_previous_index = history_for_new_value.empty? ? -1 : history_for_new_value.last[:block_index]
      last_change = history_for_new_value.empty? ? 0 : history_for_new_value.last[:diff]
      if block_index != count_previous_index
        #creates a new entry in history
        count_change_history[new_value.to_sym] << { diff: last_change + 1, block_index: block_index }
      else
        #if the same block, the update overwrites the last history
        count_change_history[new_value.to_sym].last[:diff] = last_change + 1
      end
    end

    puts "data_change_history: #{data_change_history}"
    puts "count_change_history: #{count_change_history}"
    puts "affected_data: #{affected_data}"    
  end

  def commit
    data_change_history.each do |key, change|
      if change.last[:value].nil?
        original_value = CommandExecuter.new("UNSET", key).excute        
      else
        CommandExecuter.new("SET", key, change.last[:value]).excute
      end
    end
  end

  def rollback
    affected_data.last.each do |key|
      data_change_history[key.to_sym].pop
    end
  end

  def get_from_buffer(key)
    if data_change_history[key.to_sym].nil? || data_change_history[key.to_sym].empty?
      puts "get from data base using #{key}"
      return CommandExecuter.new("GET", key).excute
    else
      #puts "data_change_history[key.to_sym]: #{data_change_history[key.to_sym]}"
      puts "get from history"
      return data_change_history[key.to_sym].last[:value]
    end
  end

  def numequalto_from_buffer(key)
    initial_count = CommandExecuter.new("NUMEQUALTO", key).excute
    if count_change_history[key.to_sym].nil?
      initial_count
    else
      initial_count + count_change_history[key.to_sym].last[:diff]
    end
  end
end


=begin
count_key = find_count_key(history_for_current_key, key)
    #if history_for_current_key.last[:value].nil?
    #  count_key = value
    #else
    #  count_key = history_for_current_key.last[:value]
    #end
    puts "count_key: #{count_key}"
    puts "value: #{value}, history_for_current_key: #{history_for_current_key}"
    count_change_history[count_key.to_sym] = [] if count_change_history[count_key.to_sym].nil?
    count_history_for_current_key = count_change_history[count_key.to_sym]
    count_previous_index = count_history_for_current_key.empty? ? -1 : count_history_for_current_key.last[:block_index]

    if block_index != count_previous_index
      puts "count_history_for_current_key: #{count_history_for_current_key}"
      last_count_change = count_history_for_current_key.empty? ? 0 : count_history_for_current_key.last[:diff]

      count_change_history[count_key.to_sym] = [] if count_change_history[count_key.to_sym].nil?
      if operation == "SET"
        count_change_history[count_key.to_sym] << { diff: last_count_change + 1, block_index: block_index }
      else
        count_change_history[count_key.to_sym] << { diff: last_count_change - 1, block_index: block_index } 
      end
      puts "count_key: #{count_key}, count_change_history[count_key.to_sym]: #{count_change_history[count_key.to_sym]}"
    else
      len = count_history_for_current_key.length
      last_count_change = count_history_for_current_key[len-2][:diff]
      if operation == "SET"
        count_change_history[count_key.to_sym].last[:diff] = last_count_change + 1
      else
        count_change_history[count_key.to_sym].last[:diff] = last_count_change - 1
      end
      puts "count_change_history[count_key.to_sym]: #{count_change_history[count_key.to_sym]}"        
      puts "history_for_current_key.last: #{history_for_current_key.last}"
      puts "history_for_current_key: #{history_for_current_key}"
      puts "count_key: #{count_key}, count_change_history[count_key.to_sym]: #{count_change_history[count_key.to_sym]}"
    end



    last_count_change = count_history_for_current_key.empty? ? 0 : count_history_for_current_key.last[:diff]

      count_change_history[count_key.to_sym] = [] if count_change_history[count_key.to_sym].nil?
      if operation == "SET"
        count_change_history[count_key.to_sym] << { diff: last_count_change + 1, block_index: block_index }
      else
        count_change_history[count_key.to_sym] << { diff: last_count_change - 1, block_index: block_index } 
      end
      puts "count_key: #{count_key}, count_change_history[count_key.to_sym]: #{count_change_history[count_key.to_sym]}"
    else
      len = count_history_for_current_key.length
      last_count_change = count_history_for_current_key[len-2][:diff]
      if operation == "SET"
        count_change_history[count_key.to_sym].last[:diff] = last_count_change + 1
      else
        count_change_history[count_key.to_sym].last[:diff] = last_count_change - 1
      end


      def all_count_keys_affected_in_current_block
    trackable_count_keys = []
    
    affected_data.each do |data_key|
      count_key = data_change_history[data_key.to_sym].last[:value]
      if count_key.nil?
        non_trackable_count_keys = backtrace_non_trackable_count_keys(data_key)
      else
        trackable_count_keys << data_change_history[data_key.to_sym].last[:value]
      end
    end
    trackable_count_keys, non_trackable_count_keys
  end

  def backtrace_non_trackable_count_keys(data_key)
    data_change_history[data_key.to_sym]
    #get original value of data from database, in the first block
    if block_index == 0
      CommandExecuter.new("GET", data_key).excute
    else

    end
  end
=end  

